package mqtt

import (
	"fmt"
	"io"
	"time"
)

type (
	propertyID uint32
	Interval   time.Duration
	BoolFlag   bool
)

type propertyType interface {
	io.WriterTo
	matchesID(id propertyID) bool
}

const (
	PayloadFormatIndicator          propertyID = 1
	MessageExpiryInterval           propertyID = 2
	ContentType                     propertyID = 3
	ResponseTopic                   propertyID = 8
	CorrelationData                 propertyID = 9
	SubscriptionIdentifier          propertyID = 11
	SessionExpiryInterval           propertyID = 17
	AssignedClientIdentifier        propertyID = 18
	ServerKeepAlive                 propertyID = 19
	AuthenticationMethod            propertyID = 21
	AuthenticationData              propertyID = 22
	RequestProblemInformation       propertyID = 23
	WillDelayInterval               propertyID = 24
	RequestResponseInformation      propertyID = 25
	ResponseInformation             propertyID = 26
	ServerReference                 propertyID = 28
	ReasonString                    propertyID = 31
	ReceiveMaximum                  propertyID = 33
	TopicAliasMaximum               propertyID = 34
	TopicAlias                      propertyID = 35
	MaximumQoS                      propertyID = 36
	RetainAvailable                 propertyID = 37
	UserProperty                    propertyID = 38
	MaximumPacketSize               propertyID = 39
	WildcardSubscriptionAvailable   propertyID = 40
	SubscriptionIdentifierAvailable propertyID = 41
	SharedSubscriptionAvailable     propertyID = 42
)

func readPropertyID(r io.Reader) (propertyID, error) {
	id, err := readVariableInt(r)
	return propertyID(id), err
}

func (i Interval) WriteTo(w io.Writer) (int64, error) {
	return FourByteInt(i).WriteTo(w)
}

func (b BoolFlag) WriteTo(w io.Writer) (int64, error) {
	if b {
		return Byte(1).WriteTo(w)
	}
	return Byte(0).WriteTo(w)
}

type Property[T propertyType] struct {
	Value T
	IsSet bool
}

func (p *Property[T]) Set(value T) {
	p.IsSet = true
	p.Value = value
}

func (p Property[T]) Get() (T, bool) {
	return p.Value, p.IsSet
}

func (p Property[T]) writeIfSet(w io.Writer, id propertyID) error {
	if !p.Value.matchesID(id) {
		panic(fmt.Sprintf("Property %q does not have type %T", id, p.Value))
	}
	if !p.IsSet {
		return nil
	}
	if _, err := id.writeTo(w); err != nil {
		return err
	}
	if _, err := p.Value.WriteTo(w); err != nil {
		return propertyWriteError(id, p, err)
	}
	return nil
}

func propertyWriteError(id propertyID, v any, err error) error {
	return fmt.Errorf("Error writing property %s with value %d: %w", id, v, err)
}

func readByteProperty(r io.Reader) (Property[Byte], error) {
	b, err := readByte(r)
	return Property[Byte]{
		Value: b,
		IsSet: true,
	}, err
}

func readTwoByteProperty(r io.Reader) (Property[TwoByteInt], error) {
	twoByte, err := readTwoByte(r)
	return Property[TwoByteInt]{
		Value: twoByte,
		IsSet: true,
	}, err
}

func readFourByteProperty(r io.Reader) (Property[FourByteInt], error) {
	fourByte, err := readFourByte(r)
	return Property[FourByteInt]{
		Value: fourByte,
		IsSet: true,
	}, err
}

func readIntervalProperty(r io.Reader) (Property[Interval], error) {
	interval, err := readFourByte(r)
	return Property[Interval]{
		Value: Interval(interval * FourByteInt(time.Second)),
		IsSet: true,
	}, err
}

func readBoolFlagProperty(r io.Reader) (Property[BoolFlag], error) {
	flag, err := readByte(r)
	if err != nil {
		return Property[BoolFlag]{}, err
	}
	if flag > 1 {
		return Property[BoolFlag]{}, ProtocolError
	}
	return Property[BoolFlag]{
		Value: BoolFlag(flag == 1),
		IsSet: true,
	}, err
}

func readUTF8StringProperty(r io.Reader) (Property[UTF8String], error) {
	s, err := readUTF8String(r)
	return Property[UTF8String]{
		Value: s,
		IsSet: true,
	}, err
}

func readBinaryDataProperty(r io.Reader) (Property[BinaryData], error) {
	d, err := readBinaryData(r)
	return Property[BinaryData]{
		Value: d,
		IsSet: true,
	}, err
}

func readAndAppendUserProperties(r io.Reader, props []UTF8StringPair) ([]UTF8StringPair, error) {
	p, err := readStringPair(r)
	if err != nil {
		return props, err
	}
	return append(props, p), nil
}

func writeUserPropertiesTo(w io.Writer, props []UTF8StringPair) error {
	for _, p := range props {
		if _, err := UserProperty.writeTo(w); err != nil {
			return err
		}

		if _, err := p.WriteTo(w); err != nil {
			return propertyWriteError(UserProperty, p, err)
		}
	}

	return nil
}

func readTopicNameProperty(r io.Reader) (Property[UTF8String], error) {
	n, err := readTopicName(r)
	if err != nil {
		return Property[UTF8String]{}, err
	}
	return Property[UTF8String]{
		Value: UTF8String(n),
		IsSet: true,
	}, err
}

func (id propertyID) String() string {
	switch id {
	case PayloadFormatIndicator:
		return "Payload Format Indicator"
	case MessageExpiryInterval:
		return "Message Expiry Interval"
	case ContentType:
		return "Content Type"
	case ResponseTopic:
		return "Response Topic"
	case CorrelationData:
		return "Correlation Data"
	case SubscriptionIdentifier:
		return "Subscription Identifier"
	case SessionExpiryInterval:
		return "Session Expiry Interval"
	case AssignedClientIdentifier:
		return "Assigned Client Identifier"
	case ServerKeepAlive:
		return "Server Keep Alive"
	case AuthenticationMethod:
		return "Authentication Method"
	case AuthenticationData:
		return "Authentication Data"
	case RequestProblemInformation:
		return "Request Problem Information"
	case WillDelayInterval:
		return "Will Delay Interval"
	case RequestResponseInformation:
		return "Request Response Information"
	case ResponseInformation:
		return "Response Information"
	case ServerReference:
		return "Server Reference"
	case ReasonString:
		return "Reason String"
	case ReceiveMaximum:
		return "Receive Maximum"
	case TopicAliasMaximum:
		return "Topic Alias Maximum"
	case TopicAlias:
		return "Topic Alias"
	case MaximumQoS:
		return "Maximum QoS"
	case RetainAvailable:
		return "Retain Available"
	case UserProperty:
		return "User Property"
	case MaximumPacketSize:
		return "Maximum Packet Size"
	case WildcardSubscriptionAvailable:
		return "Wildcard Subscription Available"
	case SubscriptionIdentifierAvailable:
		return "Subscription Identifier Available"
	case SharedSubscriptionAvailable:
		return "Shared Subscription Available"
	default:
		return "Unknown"
	}
}

func (id propertyID) writeTo(w io.Writer) (int64, error) {
	n, err := VarByteInt(id).WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("Error writing property id %d: %w", uint32(id), err)
	}
	return n, nil
}

func (Byte) matchesID(id propertyID) bool {
	return id == PayloadFormatIndicator ||
		id == RequestProblemInformation ||
		id == RequestResponseInformation ||
		id == MaximumQoS ||
		id == RetainAvailable ||
		id == WildcardSubscriptionAvailable ||
		id == SubscriptionIdentifierAvailable ||
		id == SharedSubscriptionAvailable
}

func (TwoByteInt) matchesID(id propertyID) bool {
	return id == ServerKeepAlive ||
		id == ReceiveMaximum ||
		id == TopicAliasMaximum ||
		id == TopicAlias
}

func (FourByteInt) matchesID(id propertyID) bool {
	return id == MessageExpiryInterval ||
		id == SessionExpiryInterval ||
		id == WillDelayInterval ||
		id == MaximumPacketSize
}

func (VarByteInt) matchesID(id propertyID) bool {
	return id == SubscriptionIdentifier
}

func (BinaryData) matchesID(id propertyID) bool {
	return id == CorrelationData ||
		id == AuthenticationData
}

func (UTF8String) matchesID(id propertyID) bool {
	return id == ContentType ||
		id == ResponseTopic ||
		id == AssignedClientIdentifier ||
		id == AuthenticationMethod ||
		id == ResponseInformation ||
		id == ServerReference ||
		id == ReasonString
}

func (UTF8StringPair) matchesID(id propertyID) bool {
	return id == UserProperty
}

func (BoolFlag) matchesID(id propertyID) bool {
	return Byte(0).matchesID(id)
}

func (Interval) matchesID(id propertyID) bool {
	return FourByteInt(0).matchesID(id)
}
