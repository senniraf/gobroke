package mqtt

import (
	"reflect"
	"testing"
)

func TestMatches(t *testing.T) {
	type args struct {
		topic  string
		filter string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Single level", args{"a", "a"}, true},
		{"Multi level", args{"a/b/c", "a/b/c"}, true},
		{"Single level wildcard", args{"a/b/c", "a/+/c"}, true},
		{"Multi level wildcard", args{"a/b/c", "a/#"}, true},
		{"Multi level parent wildcard", args{"a", "a/#"}, true},
		{"Single level mismatch", args{"a", "d"}, false},
		{"Multi level mismatch", args{"a/b/c", "a/b/d"}, false},
		{"Single level wildcard mismatch", args{"a/b/c", "a/+/d"}, false},
		{"Multi level wildcard mismatch", args{"a/b/c", "a/d/#"}, false},
		{"Multi level parent wildcard mismatch", args{"a", "d/#"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Matches(tt.args.topic, tt.args.filter); got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitLevels(t *testing.T) {
	type args struct {
		topic string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"Single level", args{"a"}, []string{"a"}},
		{"Multi level", args{"a/b/c"}, []string{"a", "b", "c"}},
		{"Single level wildcard", args{"a/+/c"}, []string{"a", "+", "c"}},
		{"Multi level wildcard", args{"a/#"}, []string{"a", "#"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SplitLevels(tt.args.topic); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SplitLevels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsWildcard(t *testing.T) {
	type args struct {
		topic string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"No special chars", args{"abcfaskie"}, false},
		{"Contains single level wildcard", args{"a/+"}, true},
		{"Contains multi level wildcard", args{"a/#"}, true},
		{"Contains both wildcards", args{"a/+/c/#"}, true},
		{"Contains only seperator", args{"a/b/c/d"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContainsWildcard(tt.args.topic); got != tt.want {
				t.Errorf("ContainsWildcard(%q) = %v, want %v", tt.args.topic, got, tt.want)
			}
		})
	}
}
