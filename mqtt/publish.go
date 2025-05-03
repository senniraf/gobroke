package mqtt

import (
	"io"
)

const (
	NoFormat uint8 = 0
	UTF8     uint8 = 1
)

type PublishPacket struct {
	QoS                    uint8
	Dup                    bool
	Retain                 bool
	Topic                  string
	PacketID               TwoByteInt
	Payload                Payload
	PayloadFormatIndicator Property[Byte]
	MessageExpiryInterval  Property[Interval]
	TopicAlias             Property[TwoByteInt]
	ResponseTopic          Property[UTF8String]
	CorrelationData        Property[BinaryData]
	UserProperties         []UTF8StringPair
	SubscriptionIDs        []VarByteInt
	ContentType            Property[UTF8String]
}

func ReadPublishPacket(r io.Reader, fh FixedHeader, checkPayloadFormat bool) (PublishPacket, error) {
	lr := limitedReaderFromFixedHeader(r, fh)
	p := PublishPacket{}

	p.QoS = (fh.flags >> 1) & 0b11
	p.Dup = fh.flags&0b0000_1000 != 0
	p.Retain = fh.flags&0b0000_0001 != 0

	// Check if QoS 0 and DUP is set.
	if p.QoS == 0 && p.Dup {
		return PublishPacket{}, ProtocolError
	}
	var err error
	p.Topic, err = readTopicName(&lr)
	if err != nil {
		return PublishPacket{}, err
	}

	if p.QoS > 0 {
		if p.PacketID, err = readTwoByte(&lr); err != nil {
			return PublishPacket{}, err
		}
	}

	if err := readPublishProperties(&p, &lr); err != nil {
		return PublishPacket{}, err
	}

	p.Payload = newPayload(&lr, uint32(lr.N))

	if checkPayloadFormat {
		if err := p.Payload.CheckFormat(uint8(p.PayloadFormatIndicator.Value)); err != nil {
			return PublishPacket{}, err
		}
	}
	return p, nil
}

func readPublishProperties(p *PublishPacket, r io.Reader) error {
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
		case PayloadFormatIndicator:
			p.PayloadFormatIndicator, err = readByteProperty(lr)

		case MessageExpiryInterval:
			p.MessageExpiryInterval, err = readIntervalProperty(lr)

		case TopicAlias:
			if p.TopicAlias.IsSet {
				// Topic Alias already set -> protocol error
				return ProtocolError
			}
			p.TopicAlias, err = readTwoByteProperty(lr)

		case ResponseTopic:
			if p.ResponseTopic.IsSet {
				// Response Topic already set -> protocol error
				return ProtocolError
			}
			p.ResponseTopic, err = readTopicNameProperty(lr)

		case CorrelationData:
			if p.CorrelationData.IsSet {
				// Correlation Data already set -> protocol error
				return ProtocolError
			}
			p.CorrelationData, err = readBinaryDataProperty(r)

		case UserProperty:
			p.UserProperties, err = readAndAppendUserProperties(lr, p.UserProperties)

		case SubscriptionIdentifier:
			subID, err := readVariableInt(lr)
			if err != nil {
				return err
			}
			p.SubscriptionIDs = append(p.SubscriptionIDs, subID)

		case ContentType:
			if p.ContentType.IsSet {
				// Content Type already set -> protocol error
				return ProtocolError
			}

			p.ContentType, err = readUTF8StringProperty(lr)

		default:
			// Invalid property for PUBLISH packet
			return ProtocolError
		}
		if err != nil {
			return err
		}
	}

	return nil
}
