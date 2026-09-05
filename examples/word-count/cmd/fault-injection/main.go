// This test-only wrapper injects deterministic failure or delay before
// delegating to the normal mapper. Mill itself never reads the marker:
// PostgreSQL owns the real retry policy.
package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/purinliang/mill/internal/workload"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:], "/output/.mill-faults", time.Sleep, func(args []string) error {
		command := exec.Command("/mill-word-count", args...)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		return command.Run()
	}); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, markerRoot string, pause func(time.Duration), execute func([]string) error) error {
	invocation, err := workload.ParseArgs(args)
	if err != nil {
		return err
	}
	if len(invocation.ExecutableArgs) != 1 || (invocation.ExecutableArgs[0] != "once" && invocation.ExecutableArgs[0] != "always" && invocation.ExecutableArgs[0] != "delay") {
		return errors.New("fault-injection expects -- once, -- always, or -- delay")
	}
	mode := invocation.ExecutableArgs[0]
	// Keep the initial wave alive long enough for the restart demo to kill and
	// replace the coordinator. Later shards run normally to keep the demo short.
	if mode == "delay" && invocation.ShardIndex < 3 {
		pause(15 * time.Second)
	}
	if invocation.ShardIndex == 0 {
		fail := mode == "always"
		if mode == "once" {
			// A fresh demo job gets its own marker namespace; independent jobs and
			// repeated demo runs cannot accidentally inherit a previous success.
			for _, id := range []string{invocation.JobID, invocation.TaskID} {
				if id == "." || id == ".." || filepath.Base(id) != id {
					return errors.New("demo IDs must be single path components")
				}
			}
			directory := filepath.Join(markerRoot, invocation.JobID)
			if err := os.MkdirAll(directory, 0o755); err != nil {
				return err
			}
			err := os.Mkdir(filepath.Join(directory, invocation.TaskID), 0o755)
			if err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			fail = err == nil
		}
		if fail {
			// Leave a deliberately wrong partial result. The merger must use only
			// successful attempts listed by Mill, not every output file on disk.
			uri, err := url.Parse(invocation.OutputURI)
			if err != nil || uri.Scheme != "file" || uri.Host != "" || !filepath.IsAbs(uri.Path) {
				return errors.New("fault demo requires a local file output URI")
			}
			if err := os.MkdirAll(filepath.Dir(uri.Path), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(uri.Path, []byte("{\"word\":\"injected-invalid-output\",\"count\":999}\n"), 0o644); err != nil {
				return err
			}
			return fmt.Errorf("injected failure: mode=%s shard=0 task=%s", mode, invocation.TaskID)
		}
	}
	invocation.ExecutableArgs = nil
	cleanArgs, err := invocation.CommandArgs()
	if err != nil {
		return err
	}
	return execute(cleanArgs)
}
