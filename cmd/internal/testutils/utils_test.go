package testutils

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

type contextCommand struct {
	ctx context.Context
}

func (c *contextCommand) Run(ctx context.Context) error {
	c.ctx = ctx
	fmt.Print("output")
	return nil
}

func TestCommandStdoutPassesContext(t *testing.T) {
	cmd := new(contextCommand)

	if got := CommandStdout(t, cmd); got != "output" {
		t.Fatalf("CommandStdout() = %q, want %q", got, "output")
	}
	if cmd.ctx == nil {
		t.Fatal("CommandStdout() passed a nil context")
	}
}

func TestCaptureStdoutStderrLargeOutput(t *testing.T) {
	const outputSize = 2 * 1024 * 1024
	wantStdout := strings.Repeat("o", outputSize)
	wantStderr := strings.Repeat("e", outputSize)

	stdout, stderr := CaptureStdoutStderr(func() {
		_, _ = fmt.Fprint(os.Stdout, wantStdout)
		_, _ = fmt.Fprint(os.Stderr, wantStderr)
	})

	if stdout != wantStdout {
		t.Fatalf("captured stdout length = %d, want %d", len(stdout), len(wantStdout))
	}
	if stderr != wantStderr {
		t.Fatalf("captured stderr length = %d, want %d", len(stderr), len(wantStderr))
	}
}
