package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/monitor"
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "parse":
		err = parseCommand(os.Args[2:], os.Stdout)
	case "monitor":
		err = monitorCommand(os.Args[2:])
	case "help", "-h", "--help":
		usage(os.Stdout)
		return
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "FreeProxyAPI:", err)
		os.Exit(1)
	}
}

func monitorCommand(args []string) error {
	flags := flag.NewFlagSet("monitor", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "monitor JSON configuration")
	worker := flags.String("worker-id", "", "optional stable worker identifier")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("-config is required")
	}
	config, err := monitor.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	runner, err := monitor.NewRunner(config, *worker)
	if err != nil {
		return err
	}
	defer runner.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go restoreDefaultSignalsOnDone(ctx, stop)
	return runner.Run(ctx)
}

// restoreDefaultSignalsOnDone unregisters the graceful-shutdown handler once
// the first SIGINT/SIGTERM cancels ctx, so a second signal uses the default
// disposition and terminates a drain that is taking too long.
func restoreDefaultSignalsOnDone(ctx context.Context, stop context.CancelFunc) {
	<-ctx.Done()
	stop()
}

func parseCommand(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("parse", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "local line-delimited proxy feed")
	defaultScheme := flags.String("default-scheme", "http", "scheme for host:port records")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *input == "" {
		return fmt.Errorf("-input is required")
	}
	file, err := os.Open(*input)
	if err != nil {
		return fmt.Errorf("open proxy feed: %w", err)
	}
	defer file.Close()

	accepted, rejected, byScheme, err := endpoint.ParseFeed(file, *defaultScheme)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "accepted=%d rejected=%d\n", accepted, rejected)
	schemes := make([]string, 0, len(byScheme))
	for scheme := range byScheme {
		schemes = append(schemes, scheme)
	}
	sort.Strings(schemes)
	for _, scheme := range schemes {
		fmt.Fprintf(out, "scheme=%s accepted=%d\n", scheme, byScheme[scheme])
	}
	return nil
}

func usage(out io.Writer) {
	fmt.Fprint(out, `FreeProxyAPI parses proxy inventories without network access.

Usage:
  FreeProxyAPI parse -input FILE [-default-scheme http]
  FreeProxyAPI monitor -config FILE [-worker-id ID]
`)
}
