package testutils

import (
	"context"
	"fmt"
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
