package distribution

import (
	"sync"
	"sync/atomic"

	"github.com/senniraf/gobroke/mqtt"
)

type node struct {
	// The subscribers of the node.
	subs []mqtt.Subscriber

	// The rwlock for the subscribers.
	subsLock sync.RWMutex

	// The multi-level wildcard subscribers of the node.
	mlwcSubs []mqtt.Subscriber

	// The rwlock for the multi-level wildcard subscribers.
	mlwcSubsLock sync.RWMutex

	// The children of the node.
	children map[string]*node

	// The rwlock for the children.
	childrenLock sync.RWMutex

	// The single level wildcard child of the node.
	slwcChild *node

	// The rwlock for the single level wildcard child.
	slwcChildLock sync.RWMutex

	// The retain message of the node.
	retainMsg mqtt.Message

	// The retain flag of the node.
	hasRetain atomic.Bool
}

func NewTopicTree() mqtt.Distributor {
	return newNode()
}

// Publish implements Distributor.
func (n *node) Publish(msg mqtt.Message) mqtt.Reason {
	return n.publish(mqtt.SplitLevels(msg.Topic), msg, msg.Retain)
}

// AddSubscription implements Distributor.
func (n *node) AddSubscription(topicFilter string, subscriber mqtt.Subscriber) {
	n.registerSubscriber(mqtt.SplitLevels(topicFilter), subscriber)
}

// RemoveSubscription implements Distributor.
func (n *node) RemoveSubscription(topicFilter string, subscriber mqtt.Subscriber) mqtt.Reason {
	return n.unregisterSubscriber(mqtt.SplitLevels(topicFilter), subscriber)
}

func newNode() *node {
	return &node{
		children: make(map[string]*node),
	}
}

func (n *node) registerSubscriber(filterLevels []string, sub mqtt.Subscriber) {
	if len(filterLevels) == 0 {
		n.subsLock.Lock()
		n.subs = append(n.subs, sub)
		n.subsLock.Unlock()

		if n.hasRetain.Load() {
			sub.AddMessage(n.retainMsg)
		}
		return
	}

	currentPart := filterLevels[0]
	if currentPart == mqtt.MultiLevelWildcard {
		n.registerMultilevel(sub)
		return
	}

	if currentPart == mqtt.SingleLevelWildcard {
		n.registerSingleLevel(filterLevels, sub)
		return
	}

	n.childrenLock.RLock()
	defer n.childrenLock.RUnlock()
	if n.children[currentPart] == nil {
		// We need to upgrade the lock to a write lock.
		n.childrenLock.RUnlock()
		n.childrenLock.Lock()

		// Check if the node was created in the meantime.
		if n.children[currentPart] == nil {
			n.children[currentPart] = newNode()
		}
		n.childrenLock.Unlock()

		// Reacquire the read lock.
		n.childrenLock.RLock()
	}

	n.children[currentPart].registerSubscriber(filterLevels[1:], sub)
}

func (n *node) registerSingleLevel(filterLevels []string, sub mqtt.Subscriber) {
	n.slwcChildLock.RLock()
	if n.slwcChild == nil {
		// We need to upgrade the lock to a write lock.
		n.slwcChildLock.RUnlock()
		n.slwcChildLock.Lock()

		// Check if the node was created in the meantime.
		if n.slwcChild == nil {
			n.slwcChild = newNode()
		}
		n.slwcChildLock.Unlock()

		// Reacquire the read lock.
		n.slwcChildLock.RLock()
	}

	n.slwcChild.registerSubscriber(filterLevels[1:], sub)
	n.slwcChildLock.RUnlock()

	retainMsgs := []mqtt.Message{}
	n.childrenLock.RLock()
	for _, child := range n.children {
		retainMsgs = append(retainMsgs, child.getRecursiveRetainMessages(filterLevels[1:])...)
	}
	n.childrenLock.RUnlock()

	for _, msg := range retainMsgs {
		sub.AddMessage(msg)
	}
}

func (n *node) registerMultilevel(sub mqtt.Subscriber) {
	n.mlwcSubsLock.Lock()
	n.mlwcSubs = append(n.mlwcSubs, sub)
	n.mlwcSubsLock.Unlock()

	retainMsgs := []mqtt.Message{}
	if n.hasRetain.Load() {
		retainMsgs = append(retainMsgs, n.retainMsg)
	}

	n.childrenLock.RLock()
	for _, child := range n.children {
		retainMsgs = append(retainMsgs, child.getRecursiveRetainMessages([]string{mqtt.MultiLevelWildcard})...)
	}
	n.childrenLock.RUnlock()

	for _, msg := range retainMsgs {
		sub.AddMessage(msg)
	}
}

// unregisterSubscriber removes the subscriber from the node.
func (n *node) unregisterSubscriber(filterLevels []string, sub mqtt.Subscriber) mqtt.Reason {
	if len(filterLevels) == 0 {
		n.subsLock.Lock()
		defer n.subsLock.Unlock()

		for i, s := range n.subs {
			if s == sub {
				n.subs = append(n.subs[:i], n.subs[i+1:]...)
				return mqtt.Success
			}
		}
		return mqtt.NoSubscriptionExisted
	}

	currentPart := filterLevels[0]
	if currentPart == mqtt.MultiLevelWildcard {
		n.mlwcSubsLock.Lock()
		defer n.mlwcSubsLock.Unlock()

		for i, s := range n.mlwcSubs {
			if s == sub {
				n.mlwcSubs = append(n.mlwcSubs[:i], n.mlwcSubs[i+1:]...)
				return mqtt.Success
			}
		}
		return mqtt.NoSubscriptionExisted
	}

	remainingParts := filterLevels[1:]
	if currentPart == mqtt.SingleLevelWildcard {
		n.slwcChildLock.RLock()
		defer n.slwcChildLock.RUnlock()
		if n.slwcChild == nil {
			return mqtt.NoSubscriptionExisted
		}

		reason := n.slwcChild.unregisterSubscriber(remainingParts, sub)
		if reason == mqtt.Success && n.slwcChild.isRemovable() {
			// We need to upgrade the lock to a write lock.
			n.slwcChildLock.RUnlock()
			n.slwcChildLock.Lock()

			// Check if the node was removed in the meantime and is still removable.
			if n.slwcChild != nil && n.slwcChild.isRemovable() {
				n.slwcChild = nil
			}
			n.slwcChildLock.Unlock()

			// Reacquire the read lock.
			n.slwcChildLock.RLock()
		}
		return reason
	}

	n.childrenLock.RLock()
	defer n.childrenLock.RUnlock()
	if n.children[currentPart] == nil {
		return mqtt.NoSubscriptionExisted
	}

	reason := n.children[currentPart].unregisterSubscriber(filterLevels[1:], sub)

	if reason == mqtt.Success && n.children[currentPart].isRemovable() {
		// We need to upgrade the lock to a write lock.
		n.childrenLock.RUnlock()
		n.childrenLock.Lock()

		// Check if the node was removed in the meantime and is still removable.
		if n.children[currentPart] != nil && n.children[currentPart].isRemovable() {
			delete(n.children, currentPart)
		}
		n.childrenLock.Unlock()

		// Reacquire the read lock.
		n.childrenLock.RLock()
	}

	return reason
}

func (n *node) publish(topicLevels []string, msg mqtt.Message, retain bool) mqtt.Reason {
	reason := mqtt.NoMatchingSubscribers

	n.mlwcSubsLock.RLock()
	if len(n.mlwcSubs) > 0 {
		for _, sub := range n.mlwcSubs {
			sub.AddMessage(msg)
		}
		reason = mqtt.Success
	}
	n.mlwcSubsLock.RUnlock()

	if len(topicLevels) == 0 {
		if retain {
			n.retainMsg = msg
			n.hasRetain.Store(true)
		}
		n.subsLock.RLock()
		defer n.subsLock.RUnlock()

		if len(n.subs) == 0 {
			return reason
		}

		for _, sub := range n.subs {
			sub.AddMessage(msg)
		}

		return mqtt.Success
	}

	currentPart := topicLevels[0]
	remainingParts := topicLevels[1:]

	n.childrenLock.RLock()
	child := n.children[currentPart]

	if child != nil {
		reason = child.publish(remainingParts, msg, retain)
	} else if retain {
		// We need to upgrade the lock to a write lock.
		n.childrenLock.RUnlock()
		n.childrenLock.Lock()

		// Check if the node was created in the meantime.
		if n.children[currentPart] == nil {
			n.children[currentPart] = newNode()
		}
		reason = n.children[currentPart].publish(remainingParts, msg, retain)
		n.childrenLock.Unlock()

		// Reacquire the read lock.
		n.childrenLock.RLock()
	}

	n.childrenLock.RUnlock()

	n.slwcChildLock.RLock()
	if n.slwcChild != nil {
		if r := n.slwcChild.publish(remainingParts, msg, false); r == mqtt.Success {
			reason = mqtt.Success
		}
	}
	n.slwcChildLock.RUnlock()

	return reason
}

// getRecursiveRetainMessages returns the retain messages of the node and its children.
// If topicFilter is not nil, it only returns the retain messages of the children that match the topic filter.
func (n *node) getRecursiveRetainMessages(topicFilter []string) []mqtt.Message {
	messages := []mqtt.Message{}

	if len(topicFilter) == 0 || topicFilter[0] == mqtt.MultiLevelWildcard {
		if n.hasRetain.Load() {
			messages = append(messages, n.retainMsg)
		}
	}

	if len(topicFilter) == 0 {
		return messages
	}

	currentPart := topicFilter[0]

	n.childrenLock.RLock()
	for topicPart, child := range n.children {
		if currentPart == mqtt.MultiLevelWildcard {
			messages = append(messages, child.getRecursiveRetainMessages(topicFilter)...)
			continue
		}

		if currentPart == topicPart || currentPart == mqtt.SingleLevelWildcard {
			messages = append(messages, child.getRecursiveRetainMessages(topicFilter[1:])...)
		}
	}
	n.childrenLock.RUnlock()

	return messages
}

func (n *node) isRemovable() bool {
	return len(n.subs) == 0 && len(n.mlwcSubs) == 0 && len(n.children) == 0 && n.slwcChild == nil && !n.hasRetain.Load()
}
