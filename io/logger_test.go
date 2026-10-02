package io

import (
	"fmt"
	"testing"

	"github.com/aws/smithy-go/logging"
)

func TestAwsWarnLoggerFiltersByClassification(t *testing.T) {
	tests := []struct {
		name           string
		classification logging.Classification
		message        string
		wantLogged     bool
	}{
		{name: "debug is dropped", classification: logging.Debug, message: "noisy debug", wantLogged: false},
		{name: "warn passes through", classification: logging.Warn, message: "useful warning", wantLogged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			logger := awsWarnLogger{inner: logging.LoggerFunc(func(classification logging.Classification, format string, v ...interface{}) {
				got = append(got, fmt.Sprintf(format, v...))
			})}

			logger.Logf(tt.classification, tt.message)

			if tt.wantLogged {
				if len(got) != 1 || got[0] != tt.message {
					t.Fatalf("expected [%q] to be logged, got %v", tt.message, got)
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("expected no output for %s entry, got %v", tt.classification, got)
			}
		})
	}
}
