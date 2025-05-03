package mqtt

import "strings"

const (
	SingleLevelWildcard = "+"
	MultiLevelWildcard  = "#"
	Separator           = "/"
)

func Matches(topic, filter string) bool {
	topicLevels := SplitLevels(topic)
	filterLevels := SplitLevels(filter)

	for i, filterLevel := range filterLevels {
		if filterLevel == MultiLevelWildcard {
			return true
		}

		if filterLevel == SingleLevelWildcard {
			continue
		}

		if topicLevels[i] != filterLevel {
			return false
		}
	}

	return true
}

func SplitLevels(topic string) []string {
	return strings.Split(topic, Separator)
}

func ContainsWildcard(topic string) bool {
	return strings.ContainsAny(topic, strings.Join([]string{SingleLevelWildcard, MultiLevelWildcard}, ""))
}

func IsValidTopicName(name string) bool {
	return name != "" && !ContainsWildcard(name)
}
