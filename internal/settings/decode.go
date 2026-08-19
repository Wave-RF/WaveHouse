package settings

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// decodeStrict unmarshals data into v, rejecting unknown fields and trailing
// content. Unknown fields are the JSON form of the retired-config-key trap: a
// misspelled key silently ignored quietly reverts the author's intent to a
// default, so it must be an error.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing content after the JSON document")
	}
	return nil
}

// dupKeyFindings reports duplicated object keys anywhere in data.
// encoding/json silently keeps the last duplicate, and hand-edited JSON is
// exactly where a copy-pasted block shadows the original — so the scan is a
// separate token-level pass. Parse errors are ignored here: syntactically
// invalid JSON is already reported by decodeStrict.
func dupKeyFindings(file string, data []byte) []Finding {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var findings []Finding
	// The scan error is deliberately dropped (see doc comment).
	_ = scanValue(dec, file, "", &findings)
	return findings
}

func scanValue(dec *json.Decoder, file, path string, findings *[]Finding) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil // scalar
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return err
			}
			key, _ := keyTok.(string)
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			if seen[key] {
				*findings = append(*findings, Finding{
					Severity: SeverityError,
					File:     file,
					Path:     childPath,
					Message:  "duplicate key — JSON keeps only the last occurrence, silently discarding the first",
				})
			}
			seen[key] = true
			if err := scanValue(dec, file, childPath, findings); err != nil {
				return err
			}
		}
		_, err = dec.Token() // consume '}'
		return err
	case '[':
		for i := 0; dec.More(); i++ {
			if err := scanValue(dec, file, fmt.Sprintf("%s[%d]", path, i), findings); err != nil {
				return err
			}
		}
		_, err = dec.Token() // consume ']'
		return err
	}
	return nil
}
