package distribution_test

import (
	"reflect"
	"testing"

	"github.com/senniraf/gobroke/distribution"
	"github.com/senniraf/gobroke/mqtt"
)

var testPayload = []byte("test")

var subscriptionPatterns = []struct {
	name               string
	topicName          string
	matchingFilters    []string
	mismatchingFilters []string
}{
	{
		name:               "one level",
		topicName:          "test",
		matchingFilters:    []string{"test"},
		mismatchingFilters: []string{"fail", "success", "test/test"},
	},
	{
		name:               "one level different lower vs. upper case",
		topicName:          "Test",
		matchingFilters:    []string{"Test"},
		mismatchingFilters: []string{"test", "tesT", "TEST"},
	},
	{
		name:               "separator",
		topicName:          "/",
		matchingFilters:    []string{"/"},
		mismatchingFilters: []string{"/foo", "foo/", "foo"},
	},
	{
		name:               "leading separator",
		topicName:          "/foo",
		matchingFilters:    []string{"/foo"},
		mismatchingFilters: []string{"/", "foo/", "foo"},
	},
	{
		name:               "trailing separator",
		topicName:          "foo/",
		matchingFilters:    []string{"foo/"},
		mismatchingFilters: []string{"/", "/foo", "foo"},
	},
	{
		name:               "two level",
		topicName:          "foo/bar",
		matchingFilters:    []string{"foo/bar"},
		mismatchingFilters: []string{"bar/foo", "foo", "bar", "foo/", "/bar", "foo/not", "foo/bar/foo"},
	},
	{
		name:               "one level single level wildcard",
		topicName:          "foo",
		matchingFilters:    []string{"foo", "+"},
		mismatchingFilters: []string{"foo/+", "+/foo"},
	},
	{
		name:               "two level single level wildcard",
		topicName:          "foo/bar",
		matchingFilters:    []string{"foo/bar", "foo/+", "+/bar", "+/+"},
		mismatchingFilters: []string{"+/foo", "bar/+", "+", "foo/+/bar"},
	},
	{
		name:               "three level single level wildcard",
		topicName:          "foo/bar/bear",
		matchingFilters:    []string{"foo/bar/bear", "+/bar/bear", "foo/+/bear", "foo/bar/+", "+/+/bear", "foo/+/+", "+/bar/+", "+/+/+"},
		mismatchingFilters: []string{"+/foo/bar/bear", "foo/bar/bear/+", "foo/+/bar/bear", "+", "+/bear", "foo/+"},
	},
	{
		name:               "one level multi level wildcard",
		topicName:          "foo",
		matchingFilters:    []string{"foo", "foo/#", "#"},
		mismatchingFilters: []string{"foo/bar/#"},
	},
	{
		name:               "two level multi level wildcard",
		topicName:          "foo/bar",
		matchingFilters:    []string{"foo/bar", "foo/#", "#", "foo/bar/#"},
		mismatchingFilters: []string{"foo/bar/test/#", "bar/#"},
	},
	{
		name:               "two level wildcards leading separator",
		topicName:          "/foo",
		matchingFilters:    []string{"/foo", "/#", "#", "+/+", "/+", "+/foo"},
		mismatchingFilters: []string{"foo", "/bar", "+"},
	},
}

func TestGivenSubWhenPubThenMessagesSent(t *testing.T) {
	for _, pattern := range subscriptionPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			sut := distribution.NewTopicTree()
			matchingSubs := createSubscriberMocks(pattern.matchingFilters, sut)
			mismatchingSubs := createSubscriberMocks(pattern.mismatchingFilters, sut)

			testMessage := mqtt.Message{Topic: pattern.topicName, Payload: testPayload}
			reason := sut.Publish(testMessage)

			if reason != mqtt.Success {
				t.Errorf("Expected success, got %v", reason)
			}
			for i, sub := range matchingSubs {
				if !reflect.DeepEqual(sub.messages, []mqtt.Message{testMessage}) {
					t.Errorf("Expected message %v for filter %v, got %v", testMessage, pattern.matchingFilters[i], sub.messages)
				}
			}
			for i, sub := range mismatchingSubs {
				if len(sub.messages) != 0 {
					t.Errorf("Expected no message for filter %v, got %v", pattern.mismatchingFilters[i], sub.messages)
				}
			}
		})
	}
}

func TestGivenNoMatchingSubWhenPubThenNoMatchingSubscribers(t *testing.T) {
	for _, pattern := range subscriptionPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			sut := distribution.NewTopicTree()

			mismatchingSubs := createSubscriberMocks(pattern.mismatchingFilters, sut)

			testMessage := mqtt.Message{Topic: pattern.topicName, Payload: testPayload}
			reason := sut.Publish(testMessage)

			if reason != mqtt.NoMatchingSubscribers {
				t.Errorf("Expected no matching subscribers, got %v", reason)
			}
			for i, sub := range mismatchingSubs {
				if len(sub.messages) != 0 {
					t.Errorf("Expected no message for filter %v, got %v", pattern.mismatchingFilters[i], sub.messages)
				}
			}
		})
	}
}

func TestGivenSubAndUnsubWhenPubThenNoMessagesSent(t *testing.T) {
	for _, pattern := range subscriptionPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			sut := distribution.NewTopicTree()
			matchingSubs := createSubscriberMocks(pattern.matchingFilters, sut)

			for i, sub := range matchingSubs {
				_ = sut.RemoveSubscription(pattern.matchingFilters[i], sub)
			}

			testMessage := mqtt.Message{Topic: pattern.topicName, Payload: testPayload}
			reason := sut.Publish(testMessage)

			if reason != mqtt.NoMatchingSubscribers {
				t.Errorf("Expected no matching subscribers, got %v", reason)
			}
			for i, sub := range matchingSubs {
				if len(sub.messages) != 0 {
					t.Errorf("Expected no message for filter %v, got %v", pattern.matchingFilters[i], sub.messages)
				}
			}
		})
	}
}

func TestGivenSubWhenUnsubDifferentFilterNoSubscriptionExisted(t *testing.T) {
	for _, pattern := range subscriptionPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			sut := distribution.NewTopicTree()
			matchingSubs := createSubscriberMocks(pattern.matchingFilters, sut)

			reason := sut.RemoveSubscription("fail", matchingSubs[0])

			if reason != mqtt.NoSubscriptionExisted {
				t.Errorf("Expected no subscription existed, got %v", reason)
			}
			for i, sub := range matchingSubs {
				if len(sub.messages) != 0 {
					t.Errorf("Expected no message for filter %v, got %v", pattern.matchingFilters[i], sub.messages)
				}
			}
		})
	}
}

func TestGivenRetainWhenSubThenMessageSent(t *testing.T) {
	for _, pattern := range subscriptionPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			sut := distribution.NewTopicTree()

			testMessage := mqtt.Message{Topic: pattern.topicName, Payload: testPayload, Retain: true}
			reason := sut.Publish(testMessage)

			if reason != mqtt.NoMatchingSubscribers {
				t.Errorf("Expected success, got %v", reason)
			}

			matchingSubs := createSubscriberMocks(pattern.matchingFilters, sut)
			mismatchingSubs := createSubscriberMocks(pattern.mismatchingFilters, sut)

			for i, sub := range matchingSubs {
				if !reflect.DeepEqual(sub.messages, []mqtt.Message{testMessage}) {
					t.Errorf("Expected message %v for filter %v, got %v", testMessage, pattern.matchingFilters[i], sub.messages)
				}
			}
			for i, sub := range mismatchingSubs {
				if len(sub.messages) != 0 {
					t.Errorf("Expected no message for filter %v, got %v", pattern.mismatchingFilters[i], sub.messages)
				}
			}
		})
	}
}

type mockSubscriber struct {
	messages []mqtt.Message
}

func (s *mockSubscriber) AddMessage(msg mqtt.Message) {
	s.messages = append(s.messages, msg)
}

func createSubscriberMocks(filters []string, tt mqtt.Distributor) []*mockSubscriber {
	mocks := make([]*mockSubscriber, len(filters))
	for i, filter := range filters {
		mocks[i] = &mockSubscriber{}
		tt.AddSubscription(filter, mocks[i])
	}
	return mocks
}
