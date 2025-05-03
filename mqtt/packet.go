package mqtt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

type PacketType uint8

const (
	Reserved    PacketType = 0
	CONNECT     PacketType = 1
	CONNACK     PacketType = 2
	PUBLISH     PacketType = 3
	PUBACK      PacketType = 4
	PUBREC      PacketType = 5
	PUBREL      PacketType = 6
	PUBCOMP     PacketType = 7
	SUBSCRIBE   PacketType = 8
	SUBACK      PacketType = 9
	UNSUBSCRIBE PacketType = 10
	UNSUBACK    PacketType = 11
	PINGREQ     PacketType = 12
	PINGRESP    PacketType = 13
	DISCONNECT  PacketType = 14
	AUTH        PacketType = 15
)

type (
	Byte           uint8
	TwoByteInt     uint16
	FourByteInt    uint32
	UTF8String     string
	VarByteInt     uint32
	BinaryData     []byte
	UTF8StringPair [2]UTF8String
)

const (
	MaxBinaryDataSize = math.MaxUint16
)

const (
	maxVariableInt = 268_435_455
)

const (
	bufSize = 4096
)

var byteOrder = binary.BigEndian

type FixedHeader struct {
	// The type of the packet.
	Type PacketType

	// The flags of the packet.
	flags uint8

	// Remaining length of the packet.
	RemainingLength uint32
}

func ReadFixedHeader(r io.Reader) (FixedHeader, error) {
	b, err := readByte(r)
	if err != nil {
		return FixedHeader{}, err
	}

	type_ := PacketType(b >> 4)
	if type_ == Reserved {
		return FixedHeader{}, MalformedPacket
	}

	flags := b & 0x0F
	switch type_ {
	case PUBLISH:
		break
	case PUBREL, SUBSCRIBE, UNSUBSCRIBE:
		if flags != 0b0010 {
			return FixedHeader{}, MalformedPacket
		}
	default:
		if flags != 0b0000 {
			return FixedHeader{}, MalformedPacket
		}
	}

	remainingLength, err := readVariableInt(r)
	if err != nil {
		return FixedHeader{}, err
	}

	return FixedHeader{
		Type:            type_,
		flags:           uint8(flags),
		RemainingLength: uint32(remainingLength),
	}, nil
}

var ErrRemainingLengthTooLarge = errors.New("remaining length > 268'435'455")

func (h FixedHeader) WriteTo(w io.Writer) (int64, error) {
	var b [5]byte
	b[0] = byte(h.Type<<4) | byte(h.flags)
	n := 1
	if h.RemainingLength > maxVariableInt {
		return 0, ErrRemainingLengthTooLarge
	}
	n += binary.PutUvarint(b[n:], uint64(h.RemainingLength))
	return int64(n), writeAll(w, b[:n])
}

type Payload struct {
	r      io.LimitedReader
	length uint32
}

func newPayload(r io.Reader, length uint32) Payload {
	return Payload{
		r:      io.LimitedReader{R: r, N: int64(length)},
		length: length,
	}
}

func (pl Payload) Length() uint32 {
	return pl.length
}

func (pl Payload) Read(p []byte) (n int, err error) {
	return pl.r.Read(p)
}

func (pl *Payload) CheckFormat(formatIndicator uint8) error {
	if formatIndicator != UTF8 {
		return nil
	}
	newReader := new(bytes.Buffer)
	toCheck := io.TeeReader(&pl.r, newReader)
	buffer := make([]byte, bufSize)

	var err error
	for err != nil {
		var bytesRead int
		bytesRead, err = toCheck.Read(buffer)

		nextRune := 0
		for nextRune < bytesRead {
			r, size := utf8.DecodeRune(buffer[nextRune:bytesRead])
			if r == utf8.RuneError && size == 1 {
				return PayloadFormatInvalid
			}
			nextRune += size
		}
	}

	if err != io.EOF {
		return MalformedPacket
	}

	if newReader.Len() != int(pl.length) {
		return MalformedPacket
	}

	pl.r = io.LimitedReader{R: newReader, N: int64(pl.length)}
	return nil
}

func readByte(r io.Reader) (Byte, error) {
	var b Byte
	if err := binary.Read(r, byteOrder, &b); err != nil {
		return 0, handleReadError(err)
	}
	return b, nil
}

func (b Byte) WriteTo(w io.Writer) (int64, error) {
	return 1, binary.Write(w, byteOrder, b)
}

func readTwoByte(r io.Reader) (TwoByteInt, error) {
	var v TwoByteInt
	err := handleReadError(binary.Read(r, byteOrder, &v))
	return v, err
}

func (i TwoByteInt) WriteTo(w io.Writer) (int64, error) {
	return 2, binary.Write(w, byteOrder, i)
}

func readFourByte(r io.Reader) (FourByteInt, error) {
	var v FourByteInt
	err := handleReadError(binary.Read(r, byteOrder, v))
	return v, err
}

func (i FourByteInt) WriteTo(w io.Writer) (int64, error) {
	return 4, binary.Write(w, byteOrder, i)
}

func readVariableInt(r io.Reader) (VarByteInt, error) {
	i, err := binary.ReadUvarint(bufio.NewReaderSize(r, 4))
	if err != nil {
		return 0, handleReadError(err)
	}

	if i > maxVariableInt {
		return 0, MalformedPacket
	}
	return VarByteInt(i), nil
}

func (i VarByteInt) WriteTo(w io.Writer) (int64, error) {
	if i > maxVariableInt {
		return 0, fmt.Errorf("cannot encode %d as variable int as it's > %d", i, maxVariableInt)
	}
	n, err := w.Write(binary.AppendUvarint(nil, uint64(i)))
	return int64(n), err
}

func readStringPair(r io.Reader) (UTF8StringPair, error) {
	key, err := readUTF8String(r)
	if err != nil {
		return [2]UTF8String{}, err
	}
	value, err := readUTF8String(r)
	if err != nil {
		return [2]UTF8String{}, err
	}
	return [2]UTF8String{UTF8String(key), UTF8String(value)}, nil
}

func readUTF8String(r io.Reader) (UTF8String, error) {
	b, err := readBinaryData(r)
	if err != nil {
		return "", err
	}
	// Checks for valid UTF-8 and data not in U+D800 to U+DFFF
	if !utf8.Valid(b) {
		return "", MalformedPacket
	}
	return UTF8String(b), nil
}

func (p UTF8StringPair) WriteTo(w io.Writer) (int64, error) {
	n := int64(0)
	for _, s := range p {
		m, err := s.WriteTo(w)
		n = n + m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s UTF8String) WriteTo(w io.Writer) (int64, error) {
	return BinaryData(s).WriteTo(w)
}

func readBinaryData(r io.Reader) (BinaryData, error) {
	var length uint16
	if err := binary.Read(r, byteOrder, &length); err != nil {
		return nil, handleReadError(err)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, handleReadError(err)
	}
	return data, nil
}

func (d BinaryData) WriteTo(w io.Writer) (int64, error) {
	l := len(d)
	if l > MaxBinaryDataSize {
		return 0, fmt.Errorf("binary data size too big: %d, max is %d", l, MaxBinaryDataSize)
	}
	if n, err := TwoByteInt(l).WriteTo(w); err != nil {
		return n, err
	}
	n := int64(2)
	m, err := w.Write(d)
	return n + int64(m), err
}

func limitedReaderFromFixedHeader(r io.Reader, fh FixedHeader) io.LimitedReader {
	return io.LimitedReader{R: r, N: int64(fh.RemainingLength)}
}

func readLength(r io.Reader) (*io.LimitedReader, error) {
	propertyLength, err := readVariableInt(r)
	if err != nil {
		return nil, err
	}

	return &io.LimitedReader{R: r, N: int64(propertyLength)}, err
}

func readTopicName(r io.Reader) (string, error) {
	topic, err := readUTF8String(r)
	if err != nil {
		return "", err
	}

	if !IsValidTopicName(string(topic)) {
		return "", ProtocolError
	}

	return string(topic), nil
}

func handleReadError(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = MalformedPacket
	}
	// TODO: Error handling. Log and send UnspecifiedError?
	return err
}

func writeAll(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
