package mqtt

type Reason uint8

const (
	Success                             Reason = 0x00
	NormalDisconnection                 Reason = 0x00
	GrantedQoS0                         Reason = 0x00
	GrantedQoS1                         Reason = 0x01
	GrantedQoS2                         Reason = 0x02
	DisconnectWithWillMessage           Reason = 0x04
	NoMatchingSubscribers               Reason = 0x10
	NoSubscriptionExisted               Reason = 0x11
	ContinueAuthentication              Reason = 0x18
	ReAuthenticate                      Reason = 0x19
	UnspecifiedError                    Reason = 0x80
	MalformedPacket                     Reason = 0x81
	ProtocolError                       Reason = 0x82
	ImplementationSpecificError         Reason = 0x83
	UnsupportedProtocolVersion          Reason = 0x84
	ClientIdentifierNotValid            Reason = 0x85
	BadUserNameOrPassword               Reason = 0x86
	NotAuthorized                       Reason = 0x87
	ServerUnavailable                   Reason = 0x88
	ServerBusy                          Reason = 0x89
	Banned                              Reason = 0x8A
	ServerShuttingDown                  Reason = 0x8B
	BadAuthenticationMethod             Reason = 0x8C
	KeepAliveTimeout                    Reason = 0x8D
	SessionTakenOver                    Reason = 0x8E
	TopicFilterInvalid                  Reason = 0x8F
	TopicNameInvalid                    Reason = 0x90
	PacketIdentifierInUse               Reason = 0x91
	PacketIdentifierNotFound            Reason = 0x92
	ReceiveMaximumExceeded              Reason = 0x93
	TopicAliasInvalid                   Reason = 0x94
	PacketTooLarge                      Reason = 0x95
	MessageRateTooHigh                  Reason = 0x96
	QuotaExceeded                       Reason = 0x97
	AdministrativeAction                Reason = 0x98
	PayloadFormatInvalid                Reason = 0x99
	RetainNotSupported                  Reason = 0x9A
	QoSNotSupported                     Reason = 0x9B
	UseAnotherServer                    Reason = 0x9C
	ServerMoved                         Reason = 0x9D
	SharedSubscriptionsNotSupported     Reason = 0x9E
	ConnectionRateExceeded              Reason = 0x9F
	MaximumConnectTime                  Reason = 0xA0
	SubscriptionIdentifiersNotSupported Reason = 0xA1
	WildcardSubscriptionsNotSupported   Reason = 0xA2
)

func (r Reason) String() string {
	switch r {
	case Success:
		return "Success or Normal disconnection or Granted QoS 0"
	case GrantedQoS1:
		return "Granted QoS 1"
	case GrantedQoS2:
		return "Granted QoS 2"
	case DisconnectWithWillMessage:
		return "Disconnect with Will Message"
	case NoMatchingSubscribers:
		return "No matching subscribers"
	case NoSubscriptionExisted:
		return "No subscription existed"
	case ContinueAuthentication:
		return "Continue authentication"
	case ReAuthenticate:
		return "Re-authenticate"
	case UnspecifiedError:
		return "Unspecified error"
	case MalformedPacket:
		return "Malformed Packet"
	case ProtocolError:
		return "Protocol Error"
	case ImplementationSpecificError:
		return "Implementation specific error"
	case UnsupportedProtocolVersion:
		return "Unsupported Protocol Version"
	case ClientIdentifierNotValid:
		return "Client Identifier not valid"
	case BadUserNameOrPassword:
		return "Bad User Name or Password"
	case NotAuthorized:
		return "Not authorized"
	case ServerUnavailable:
		return "Server unavailable"
	case ServerBusy:
		return "Server busy"
	case Banned:
		return "Banned"
	case ServerShuttingDown:
		return "Server shutting down"
	case BadAuthenticationMethod:
		return "Bad authentication method"
	case KeepAliveTimeout:
		return "Keep Alive timeout"
	case SessionTakenOver:
		return "Session taken over"
	case TopicFilterInvalid:
		return "Topic Filter invalid"
	case TopicNameInvalid:
		return "Topic Name invalid"
	case PacketIdentifierInUse:
		return "Packet Identifier in use"
	case PacketIdentifierNotFound:
		return "Packet Identifier not found"
	case ReceiveMaximumExceeded:
		return "Receive Maximum exceeded"
	case TopicAliasInvalid:
		return "Topic Alias invalid"
	case PacketTooLarge:
		return "Packet too large"
	case MessageRateTooHigh:
		return "Message rate too high"
	case QuotaExceeded:
		return "Quota exceeded"
	case AdministrativeAction:
		return "Administrative action"
	case PayloadFormatInvalid:
		return "Payload format invalid"
	case RetainNotSupported:
		return "Retain not supported"
	case QoSNotSupported:
		return "QoS not supported"
	case UseAnotherServer:
		return "Use another server"
	case ServerMoved:
		return "Server moved"
	case SharedSubscriptionsNotSupported:
		return "Shared Subscriptions not supported"
	case ConnectionRateExceeded:
		return "Connection rate exceeded"
	case MaximumConnectTime:
		return "Maximum connect time"
	case SubscriptionIdentifiersNotSupported:
		return "Subscription Identifiers not supported"
	case WildcardSubscriptionsNotSupported:
		return "Wildcard Subscriptions not supported"
	default:
		return "Unknown reason"
	}
}

func (r Reason) Error() string {
	return r.String()
}
