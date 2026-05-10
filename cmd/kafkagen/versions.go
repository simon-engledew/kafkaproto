package main

import (
	"fmt"
	"strconv"
	"strings"
)

// VersionRange represents a Kafka spec version range like "0+", "3-5", "7", or "none".
type VersionRange struct {
	None bool
	Min  int
	Max  int // -1 means unbounded
}

func ParseVersionRange(s string) (VersionRange, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "none" {
		return VersionRange{None: true}, nil
	}
	if strings.HasSuffix(s, "+") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return VersionRange{}, fmt.Errorf("parse %q: %w", s, err)
		}
		return VersionRange{Min: n, Max: -1}, nil
	}
	if i := strings.IndexByte(s, '-'); i >= 0 {
		a, err := strconv.Atoi(s[:i])
		if err != nil {
			return VersionRange{}, fmt.Errorf("parse %q: %w", s, err)
		}
		b, err := strconv.Atoi(s[i+1:])
		if err != nil {
			return VersionRange{}, fmt.Errorf("parse %q: %w", s, err)
		}
		return VersionRange{Min: a, Max: b}, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return VersionRange{}, fmt.Errorf("parse %q: %w", s, err)
	}
	return VersionRange{Min: n, Max: n}, nil
}

// Condition emits a Go boolean expression that is true when varName is inside r.
// Returns "true" / "false" for the trivial cases.
func (r VersionRange) Condition(varName string) string {
	if r.None {
		return "false"
	}
	if r.Max < 0 {
		if r.Min <= 0 {
			return "true"
		}
		return fmt.Sprintf("%s >= %d", varName, r.Min)
	}
	if r.Min == r.Max {
		return fmt.Sprintf("%s == %d", varName, r.Min)
	}
	if r.Min <= 0 {
		return fmt.Sprintf("%s <= %d", varName, r.Max)
	}
	return fmt.Sprintf("%s >= %d && %s <= %d", varName, r.Min, varName, r.Max)
}

// Intersect returns the intersection of r and o. If either is None, the result is None.
func (r VersionRange) Intersect(o VersionRange) VersionRange {
	if r.None || o.None {
		return VersionRange{None: true}
	}
	out := VersionRange{Min: r.Min, Max: r.Max}
	if o.Min > out.Min {
		out.Min = o.Min
	}
	if out.Max < 0 {
		out.Max = o.Max
	} else if o.Max >= 0 && o.Max < out.Max {
		out.Max = o.Max
	}
	if out.Max >= 0 && out.Min > out.Max {
		return VersionRange{None: true}
	}
	return out
}
