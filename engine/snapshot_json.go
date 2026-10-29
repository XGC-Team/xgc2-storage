package engine

import (
	"bytes"
	"context"
)

// Large equality values are only needed for syntax and their JSON type. This
// validator walks borrowed bytes without materializing a value; periodic
// cancellation checks let the input owner release the caller promptly. The
// nesting limit matches encoding/json's existing 10,000-level limit.
type equalityJSONScanner struct {
	ctx  context.Context
	raw  []byte
	pos  int
	next int
	err  error
}

func validateLargeEqualityJSON(ctx context.Context, raw []byte) error {
	s := equalityJSONScanner{ctx: ctx, raw: raw}
	if !s.value(0) || !s.space() {
		if s.err != nil {
			return s.err
		}
		return fail("invalid_argument", "invalid equality scalar")
	}
	if s.pos != len(raw) {
		return fail("invalid_argument", "one equality scalar required")
	}
	return nil
}

func jsonSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func (s *equalityJSONScanner) check() bool {
	if s.pos >= s.next {
		s.next = s.pos + 4096
		if err := s.ctx.Err(); err != nil {
			s.err = err
			return false
		}
	}
	return true
}

func (s *equalityJSONScanner) space() bool {
	for s.pos < len(s.raw) && jsonSpace(s.raw[s.pos]) {
		if !s.check() {
			return false
		}
		s.pos++
	}
	return true
}

func (s *equalityJSONScanner) take(b byte) bool {
	if s.pos < len(s.raw) && s.raw[s.pos] == b {
		s.pos++
		return true
	}
	return false
}

func (s *equalityJSONScanner) text() bool {
	if !s.take('"') {
		return false
	}
	for s.pos < len(s.raw) {
		if !s.check() {
			return false
		}
		b := s.raw[s.pos]
		s.pos++
		switch {
		case b == '"':
			return true
		case b < 0x20:
			return false
		case b == '\\':
			if s.pos == len(s.raw) {
				return false
			}
			escaped := s.raw[s.pos]
			s.pos++
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if len(s.raw)-s.pos < 4 {
					return false
				}
				for _, digit := range s.raw[s.pos : s.pos+4] {
					if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F') {
						return false
					}
				}
				s.pos += 4
			default:
				return false
			}
		}
	}
	return false
}

func (s *equalityJSONScanner) digits() bool {
	start := s.pos
	for s.pos < len(s.raw) && s.raw[s.pos] >= '0' && s.raw[s.pos] <= '9' {
		if !s.check() {
			return false
		}
		s.pos++
	}
	return s.pos > start
}

func (s *equalityJSONScanner) number() bool {
	s.take('-')
	if !s.take('0') && !s.digits() {
		return false
	}
	if s.take('.') && !s.digits() {
		return false
	}
	if s.take('e') || s.take('E') {
		if !s.take('+') {
			s.take('-')
		}
		if !s.digits() {
			return false
		}
	}
	return true
}

func (s *equalityJSONScanner) value(depth int) bool {
	if !s.space() || !s.check() || s.pos == len(s.raw) {
		return false
	}
	switch s.raw[s.pos] {
	case '"':
		return s.text()
	case 'n', 't', 'f':
		for _, literal := range [...]string{"null", "true", "false"} {
			if bytes.HasPrefix(s.raw[s.pos:], []byte(literal)) {
				s.pos += len(literal)
				return true
			}
		}
		return false
	case '[':
		if depth >= 10000 {
			return false
		}
		s.pos++
		if !s.space() {
			return false
		}
		if s.take(']') {
			return true
		}
		for {
			if !s.value(depth+1) || !s.space() {
				return false
			}
			if s.take(']') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	case '{':
		if depth >= 10000 {
			return false
		}
		s.pos++
		if !s.space() {
			return false
		}
		if s.take('}') {
			return true
		}
		for {
			if !s.space() || !s.text() || !s.space() || !s.take(':') || !s.value(depth+1) || !s.space() {
				return false
			}
			if s.take('}') {
				return true
			}
			if !s.take(',') {
				return false
			}
		}
	default:
		if s.raw[s.pos] == '-' || s.raw[s.pos] >= '0' && s.raw[s.pos] <= '9' {
			return s.number()
		}
		return false
	}
}
