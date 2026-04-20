package rand

import "testing"

func TestID_Length(t *testing.T) {
	for _, n := range []int{1, 4, 8, 16, 32} {
		id, err := ID(n)
		if err != nil {
			t.Fatalf("ID(%d) returned error: %v", n, err)
		}

		if len(id) != n {
			t.Errorf("ID(%d) returned length %d", n, len(id))
		}
	}
}

func TestID_Charset(t *testing.T) {
	validChars := map[byte]bool{}
	for _, b := range charset {
		validChars[b] = true
	}

	id, err := ID(100)
	if err != nil {
		t.Fatalf("ID returned error: %v", err)
	}

	for i, b := range []byte(id) {
		if !validChars[b] {
			t.Errorf("invalid char %q at position %d", b, i)
		}
	}
}

func TestID_Uniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for range 100 {
		id, err := ID(8)
		if err != nil {
			t.Fatalf("ID returned error: %v", err)
		}

		if seen[id] {
			t.Errorf("duplicate ID generated: %s", id)
		}

		seen[id] = true
	}
}

func TestID_Zero(t *testing.T) {
	id, err := ID(0)
	if err != nil {
		t.Fatalf("ID(0) returned error: %v", err)
	}

	if id != "" {
		t.Errorf("ID(0) should return empty string, got %q", id)
	}
}
