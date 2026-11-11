//go:build unix

package main

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInterruptContext(t *testing.T) {
	ctx, stop := interruptContext(syscall.SIGUSR1)
	defer stop()

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGUSR1))
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context not cancelled by the signal")
	}
}

// A repeat inside signalGrace is absorbed, a later signal gets the default action; checked in a child process.
func TestInterruptContextLaterSignalKills(t *testing.T) {
	if os.Getenv("PARQUET_TOOLS_INTERRUPT_CHILD") == "1" {
		// the repeat lands well inside the grace period but long after an immediate unregister would have run
		signalGrace = 500 * time.Millisecond
		ctx, stop := interruptContext(syscall.SIGTERM)
		defer stop()
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		time.Sleep(100 * time.Millisecond)
		_, _ = os.Stdout.WriteString("survived-repeat\n")
		for range 200 {
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			time.Sleep(25 * time.Millisecond)
		}
		os.Exit(0)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestInterruptContextLaterSignalKills$")
	cmd.Env = append(os.Environ(), "PARQUET_TOOLS_INTERRUPT_CHILD=1")
	out, err := cmd.Output()
	require.True(t, strings.Contains(string(out), "survived-repeat"), "a repeat inside the grace period killed the child")
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled(), "child survived a signal after the grace period")
	require.Equal(t, syscall.SIGTERM, status.Signal())
}
