package kafka2

import "testing"

func TestTopicBuilder(t *testing.T) {
	b := NewTopicBuilder("order", "v1")
	if got := b.Event("created"); got != "order.created.v1" {
		t.Fatalf("got %s", got)
	}
	if got := RetryTopic("order.created.v1"); got != "order.created.v1.retry" {
		t.Fatalf("retry: got %s", got)
	}
	if got := DLQTopic("order.created.v1"); got != "order.created.v1.dlq" {
		t.Fatalf("dlq: got %s", got)
	}
}

func TestValidateTopicName(t *testing.T) {
	cases := []struct {
		name    string
		valid   bool
	}{
		{"order.created.v1", true},
		{"user.registered.v1", true},
		{"order.created.v1.retry", true},
		{"order.created.v1.dlq", true},
		{"Invalid", false},
	}
	for _, tc := range cases {
		err := ValidateTopicName(tc.name)
		if tc.valid && err != nil {
			t.Fatalf("%s should be valid: %v", tc.name, err)
		}
		if !tc.valid && err == nil {
			t.Fatalf("%s should be invalid", tc.name)
		}
	}
}
