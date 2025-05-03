package mqtt

import (
	"io"
	"time"
)

const (
	ProtocolName       = "MQTT"
	SessionNotExpiring = time.Duration(0xFFFFFFFF) * time.Second
)

type ConnectPacket struct {
	Version                    Byte
	flags                      Byte
	KeepAlive                  time.Duration
	SessionExpiryInterval      Property[Interval]
	ReceiveMaximum             Property[TwoByteInt]
	MaxPacketSize              Property[FourByteInt]
	TopicAliasMaximum          Property[TwoByteInt]
	RequestResponseInformation Property[BoolFlag]
	RequestProblemInformation  Property[BoolFlag]
	UserProperties             []UTF8StringPair
	AuthMethod                 Property[UTF8String]
	ClientID                   UTF8String
	willMsg                    WillMessage
	userName                   UTF8String
	password                   []byte
}

type WillMessage struct {
	WillDelayInterval      Property[Interval]
	PayloadFormatIndicator Property[Byte]
	MessageExpiryInterval  Property[Interval]
	ContentType            Property[UTF8String]
	ResponseTopic          Property[UTF8String]
	correlationData        []byte
	UserProperties         []UTF8StringPair
	Topic                  string
	Payload                []byte
}

func (p ConnectPacket) CleanStart() bool {
	return (p.flags & 0b0000_0010) != 0
}

func (p ConnectPacket) WillFlag() bool {
	return (p.flags & 0b0000_0100) != 0
}

func (p ConnectPacket) WillQoS() uint8 {
	return uint8((p.flags & 0b0001_1000) >> 3)
}

func (p ConnectPacket) WillRetain() bool {
	return (p.flags & 0b0010_0000) != 0
}

func (p ConnectPacket) PasswordFlag() bool {
	return (p.flags & 0b0100_0000) != 0
}

func (p ConnectPacket) UserNameFlag() bool {
	return (p.flags & 0b1000_0000) != 0
}

func (p ConnectPacket) UserName() (UTF8String, bool) {
	return p.userName, p.UserNameFlag()
}

func (p ConnectPacket) Password() ([]byte, bool) {
	return p.password, p.PasswordFlag()
}

func (m WillMessage) CorrelationData() ([]byte, bool) {
	return m.correlationData, m.correlationData != nil
}

func ReadConnectPacket(r io.Reader, fh FixedHeader, checkPayloadFormat bool) (ConnectPacket, error) {
	lr := limitedReaderFromFixedHeader(r, fh)
	p := ConnectPacket{}

	name, err := readUTF8String(&lr)
	if err != nil {
		return ConnectPacket{}, err
	}
	if name != ProtocolName {
		return ConnectPacket{}, ProtocolError
	}

	if p.Version, err = readByte(r); err != nil {
		return ConnectPacket{}, err
	}

	if p.flags, err = readByte(r); err != nil {
		return ConnectPacket{}, err
	}

	// Check reserved bit
	if p.flags&0b0000_0001 != 0 {
		return ConnectPacket{}, MalformedPacket
	}

	if p.WillQoS() == 3 {
		return ConnectPacket{}, MalformedPacket
	}

	if p.WillRetain() && !p.WillFlag() {
		return ConnectPacket{}, MalformedPacket
	}

	keepAlive, err := readTwoByte(&lr)
	if err != nil {
		return ConnectPacket{}, err
	}
	p.KeepAlive = time.Duration(keepAlive) * time.Second

	if err := readConnectProperties(&p, &lr); err != nil {
		return ConnectPacket{}, err
	}

	p.ClientID, err = readUTF8String(r)
	if err != nil {
		return ConnectPacket{}, err
	}

	if p.WillFlag() {
		if err := readWillProperties(&p.willMsg, &lr); err != nil {
			return ConnectPacket{}, err
		}
		p.willMsg.Topic, err = readTopicName(&lr)
		if err != nil {
			return ConnectPacket{}, err
		}
		p.willMsg.Payload, err = readBinaryData(&lr)
		if err != nil {
			return ConnectPacket{}, err
		}
	}

	if p.UserNameFlag() {
		p.userName, err = readUTF8String(&lr)
		if err != nil {
			return ConnectPacket{}, err
		}
	}

	if p.PasswordFlag() {
		p.password, err = readBinaryData(r)
		if err != nil {
			return ConnectPacket{}, err
		}
	}

	return p, nil
}

func readConnectProperties(p *ConnectPacket, r io.Reader) error {
	lr, err := readLength(r)
	if err != nil {
		return err
	}

	// Variables indicating whether a property has already
	// been received
	for lr.N > 0 {
		id, err := readPropertyID(lr)
		if err != nil {
			return err
		}

		switch id {
		case SessionExpiryInterval:
			if p.SessionExpiryInterval.IsSet {
				return ProtocolError
			}
			p.SessionExpiryInterval, err = readIntervalProperty(lr)

		case ReceiveMaximum:
			if p.ReceiveMaximum.IsSet {
				return ProtocolError
			}
			p.ReceiveMaximum, err = readTwoByteProperty(lr)
			if err != nil {
				return err
			}
			if p.ReceiveMaximum.Value == 0 {
				return ProtocolError
			}

		case MaximumPacketSize:
			if p.MaxPacketSize.IsSet {
				return ProtocolError
			}
			p.MaxPacketSize, err = readFourByteProperty(lr)
			if err != nil {
				return err
			}
			if p.MaxPacketSize.Value == 0 {
				return ProtocolError
			}

		case TopicAliasMaximum:
			if p.TopicAliasMaximum.IsSet {
				return ProtocolError
			}
			p.TopicAliasMaximum, err = readTwoByteProperty(lr)

		case RequestResponseInformation:
			if p.RequestResponseInformation.IsSet {
				return ProtocolError
			}
			p.RequestResponseInformation, err = readBoolFlagProperty(lr)

		case RequestProblemInformation:
			if p.RequestProblemInformation.IsSet {
				return ProtocolError
			}
			p.RequestProblemInformation, err = readBoolFlagProperty(lr)

		case UserProperty:
			p.UserProperties, err = readAndAppendUserProperties(lr, p.UserProperties)

		case AuthenticationMethod:
			if p.AuthMethod.IsSet {
				return ProtocolError
			}
			p.AuthMethod, err = readUTF8StringProperty(lr)

		default:
			// Invalid property for CONNECT packet
			return ProtocolError
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func readWillProperties(m *WillMessage, r io.Reader) error {
	lr, err := readLength(r)
	if err != nil {
		return err
	}

	for lr.N > 0 {
		id, err := readPropertyID(lr)
		if err != nil {
			return err
		}

		switch id {
		case WillDelayInterval:
			if m.WillDelayInterval.IsSet {
				return ProtocolError
			}
			m.WillDelayInterval, err = readIntervalProperty(lr)

		case PayloadFormatIndicator:
			if m.PayloadFormatIndicator.IsSet {
				return ProtocolError
			}
			m.PayloadFormatIndicator, err = readByteProperty(r)

		case MessageExpiryInterval:
			if m.MessageExpiryInterval.IsSet {
				return ProtocolError
			}
			m.MessageExpiryInterval, err = readIntervalProperty(lr)

		case ContentType:
			if m.ContentType.IsSet {
				return ProtocolError
			}
			m.ContentType, err = readUTF8StringProperty(lr)

		case ResponseTopic:
			if m.ResponseTopic.IsSet {
				return ProtocolError
			}
			m.ResponseTopic, err = readTopicNameProperty(lr)

		case CorrelationData:
			if _, ok := m.CorrelationData(); ok {
				// Correlation Data already set -> protocol error
				return ProtocolError
			}
			m.correlationData, err = readBinaryData(lr)

		case UserProperty:
			m.UserProperties, err = readAndAppendUserProperties(lr, m.UserProperties)
		default:
			// Invalid property for Will Properties
			return ProtocolError
		}

		if err != nil {
			return err
		}
	}

	return nil
}
