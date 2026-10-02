package io

import (
	"os"

	"github.com/aws/smithy-go/logging"
)

// awsWarnLogger passes only Warn entries through, since LoadDefaultConfig
// installs a debug-level stderr logger that would clutter CLI output.
type awsWarnLogger struct {
	inner logging.Logger
}

func (l awsWarnLogger) Logf(classification logging.Classification, format string, v ...interface{}) {
	if classification == logging.Warn {
		l.inner.Logf(classification, format, v...)
	}
}

// newAWSWarnLogger builds the logger handed to AWS SDK clients.
func newAWSWarnLogger() logging.Logger {
	return awsWarnLogger{inner: logging.NewStandardLogger(os.Stderr)}
}
