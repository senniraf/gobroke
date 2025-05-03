package mqtt

type Subscriber interface {
	// AddMessage is called when a message is published to a topic.
	AddMessage(msg Message)
}

type Distributor interface {
	// AddSubscription registers a subscriber to a topic.
	AddSubscription(topicFilter string, sub Subscriber)

	// RemoveSubscription unregisters a subscriber from a topic.
	RemoveSubscription(topicFilter string, sub Subscriber) Reason

	// Publish publishes a message to a topic.
	Publish(msg Message) Reason
}
