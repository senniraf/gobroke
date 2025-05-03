package mqtt

import (
	"bytes"
	"fmt"
	"io"
)

type ConnackPacket struct {
	SessionPresent            bool
	ReasonCode                Reason
	SessionExpiryInterval     Property[Interval]
	ReceiveMaximum            Property[TwoByteInt]
	MaximumQoS                Property[Byte]
	RetainAvailable           Property[BoolFlag]
	MaximumPacketSize         Property[FourByteInt]
	AssignedCllientIdentifier Property[UTF8String]
	TopicAliasMaximum         Property[TwoByteInt]
	ReasonString              Property[UTF8String]
	UserProperties            []UTF8StringPair
	WildcardSubAvailable      Property[BoolFlag]
	SubIdentifiersAvailable   Property[BoolFlag]
	SharedSubAvailable        Property[BoolFlag]
	ServerKeepAlive           Property[TwoByteInt]
	ResponseInformation       Property[UTF8String]
	ServerReference           Property[UTF8String]
	AuthMethod                Property[UTF8String]
	AuthData                  Property[BinaryData]
}

func (p ConnackPacket) WriteTo(w io.Writer) (int64, error) {
	propBuffer := bytes.Buffer{}
	if err := p.writeProperties(&propBuffer); err != nil {
		return 0, fmt.Errorf("Error writing connack properties to buffer: %w", err)
	}
	if l := propBuffer.Len(); l > maxVariableInt {
		return 0, fmt.Errorf("properties to long, length: %d, max is: %d", l, maxVariableInt)
	}
	propLength := VarByteInt(propBuffer.Len())

	connackFlags := uint8(0)
	if p.ReasonCode == 0 && p.SessionPresent {
		connackFlags = uint8(1)
	}

	vhPrefix := bytes.NewBuffer([]byte{connackFlags, byte(p.ReasonCode)}) // contains everything in Variable Header except props
	if _, err := propLength.WriteTo(vhPrefix); err != nil {
		return 0, fmt.Errorf("Error writing property length to Variable Header: %w", err)
	}

	l := vhPrefix.Len() + propBuffer.Len() // Variable Header Length
	if l > maxVariableInt {
		return 0, fmt.Errorf("variable header to long, lenght %d, max is %d", l, maxVariableInt)
	}

	fh := FixedHeader{
		Type:            CONNACK,
		RemainingLength: uint32(l),
	}
	bytesWritten := int64(0)

	b, err := fh.WriteTo(w)
	bytesWritten += b
	if err != nil {
		return bytesWritten, fmt.Errorf("Error writing fixed header: %w", err)
	}

	b1, err := w.Write(vhPrefix.Bytes())
	bytesWritten += int64(b1)
	if err != nil {
		return bytesWritten, fmt.Errorf("Error writing variable header: %w", err)
	}

	b1, err = w.Write(propBuffer.Bytes())
	bytesWritten += int64(b1)
	if err != nil {
		return bytesWritten, fmt.Errorf("Error writing variable header: %w", err)
	}

	return bytesWritten, nil
}

func (p ConnackPacket) writeProperties(w io.Writer) error {
	if err := p.SessionExpiryInterval.writeIfSet(w, SessionExpiryInterval); err != nil {
		return err
	}

	if err := p.ReceiveMaximum.writeIfSet(w, ReceiveMaximum); err != nil {
		return err
	}

	if err := p.MaximumQoS.writeIfSet(w, MaximumQoS); err != nil {
		return err
	}

	if err := p.RetainAvailable.writeIfSet(w, RetainAvailable); err != nil {
		return err
	}

	if err := p.MaximumPacketSize.writeIfSet(w, MaximumPacketSize); err != nil {
		return err
	}

	if err := p.AssignedCllientIdentifier.writeIfSet(w, AssignedClientIdentifier); err != nil {
		return err
	}

	if err := p.TopicAliasMaximum.writeIfSet(w, TopicAliasMaximum); err != nil {
		return err
	}

	if err := p.ReasonString.writeIfSet(w, ReasonString); err != nil {
		return err
	}

	if err := writeUserPropertiesTo(w, p.UserProperties); err != nil {
		return err
	}

	if err := p.WildcardSubAvailable.writeIfSet(w, WildcardSubscriptionAvailable); err != nil {
		return err
	}

	if err := p.SubIdentifiersAvailable.writeIfSet(w, SubscriptionIdentifierAvailable); err != nil {
		return err
	}

	if err := p.SharedSubAvailable.writeIfSet(w, SharedSubscriptionAvailable); err != nil {
		return err
	}
	return nil
}
