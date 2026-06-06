package kafka2

import (
	"fmt"
	"regexp"
	"strings"
)

var topicNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+\.v[0-9]+$`)

// TopicBuilder constructs topic names following the {domain}.{event}.{version} convention.
type TopicBuilder struct {
	domain  string
	version string
}

// NewTopicBuilder creates a builder for a bounded context, e.g. domain="order", version="v1".
func NewTopicBuilder(domain, version string) *TopicBuilder {
	return &TopicBuilder{domain: domain, version: version}
}

// Event returns the topic name for an event, e.g. "order.created.v1".
func (b *TopicBuilder) Event(event string) string {
	return fmt.Sprintf("%s.%s.%s", b.domain, event, b.version)
}

// Retry returns the retry topic for a source topic.
func RetryTopic(source string) string {
	return source + ".retry"
}

// DLQTopic returns the dead-letter topic for a source topic.
func DLQTopic(source string) string {
	return source + ".dlq"
}

// ValidateTopicName checks whether name follows the naming convention.
func ValidateTopicName(name string) error {
	if !topicNamePattern.MatchString(name) && !isInternalTopic(name) {
		return fmt.Errorf("kafka2: invalid topic name %q: expected {domain}.{event}.v{N}", name)
	}
	return nil
}

func isInternalTopic(name string) bool {
	return strings.HasSuffix(name, ".retry") || strings.HasSuffix(name, ".dlq")
}
