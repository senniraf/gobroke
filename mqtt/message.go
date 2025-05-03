package mqtt

// Message represents an application message.
type Message struct {
	// The topic of the message.
	Topic string

	// The retain flag of the message.
	Retain bool

	// The payload of the message.
	Payload []byte
}
