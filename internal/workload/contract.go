package workload

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const (
	jobIDFlag          = "job-id"
	taskIDFlag         = "task-id"
	shardIndexFlag     = "shard-index"
	inputURIFlag       = "input-uri"
	inputStartByteFlag = "input-start-byte"
	inputEndByteFlag   = "input-end-byte"
	outputURIFlag      = "output-uri"
)

// Invocation is the information Mill passes to one workload execution.
// ExecutableArgs contains the user's original arguments after the -- separator.
type Invocation struct {
	JobID          string
	TaskID         string
	ShardIndex     int
	InputURI       string
	InputStartByte int64
	InputEndByte   int64
	OutputURI      string
	ExecutableArgs []string
}

// CommandArgs serializes an invocation into the stable Mill CLI contract.
func (invocation Invocation) CommandArgs() ([]string, error) {
	if err := invocation.Validate(); err != nil {
		return nil, err
	}

	arguments := []string{
		"--" + jobIDFlag, invocation.JobID,
		"--" + taskIDFlag, invocation.TaskID,
		"--" + shardIndexFlag, strconv.Itoa(invocation.ShardIndex),
		"--" + inputURIFlag, invocation.InputURI,
		"--" + inputStartByteFlag, strconv.FormatInt(invocation.InputStartByte, 10),
		"--" + inputEndByteFlag, strconv.FormatInt(invocation.InputEndByte, 10),
		"--" + outputURIFlag, invocation.OutputURI,
		"--",
	}
	return append(arguments, invocation.ExecutableArgs...), nil
}

// ParseArgs parses the Mill CLI contract received by a workload entrypoint.
func ParseArgs(arguments []string) (Invocation, error) {
	separator := slices.Index(arguments, "--")
	if separator < 0 {
		return Invocation{}, errors.New("Mill arguments must end with a -- separator")
	}

	flags := flag.NewFlagSet("mill-workload", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var invocation Invocation
	flags.StringVar(&invocation.JobID, jobIDFlag, "", "Mill job ID")
	flags.StringVar(&invocation.TaskID, taskIDFlag, "", "Mill task ID")
	flags.IntVar(&invocation.ShardIndex, shardIndexFlag, -1, "logical shard index")
	flags.StringVar(&invocation.InputURI, inputURIFlag, "", "input URI")
	flags.Int64Var(&invocation.InputStartByte, inputStartByteFlag, -1, "inclusive input byte offset")
	flags.Int64Var(&invocation.InputEndByte, inputEndByteFlag, -1, "exclusive input byte offset")
	flags.StringVar(&invocation.OutputURI, outputURIFlag, "", "output URI")
	if err := flags.Parse(arguments[:separator]); err != nil {
		return Invocation{}, fmt.Errorf("parse Mill arguments: %w", err)
	}
	if len(flags.Args()) != 0 {
		return Invocation{}, errors.New("Mill arguments before -- must be named flags")
	}

	invocation.ExecutableArgs = append([]string(nil), arguments[separator+1:]...)
	if invocation.ExecutableArgs == nil {
		invocation.ExecutableArgs = []string{}
	}
	if err := invocation.Validate(); err != nil {
		return Invocation{}, err
	}
	return invocation, nil
}

func (invocation Invocation) Validate() error {
	if strings.TrimSpace(invocation.JobID) == "" {
		return errors.New("Mill job ID is required")
	}
	if strings.TrimSpace(invocation.TaskID) == "" {
		return errors.New("Mill task ID is required")
	}
	if invocation.ShardIndex < 0 {
		return errors.New("Mill shard index must be non-negative")
	}
	if invocation.InputStartByte < 0 {
		return errors.New("Mill input start byte must be non-negative")
	}
	if invocation.InputEndByte <= invocation.InputStartByte {
		return errors.New("Mill input end byte must be greater than its start byte")
	}
	if err := validateAbsoluteURI("input", invocation.InputURI); err != nil {
		return err
	}
	if err := validateAbsoluteURI("output", invocation.OutputURI); err != nil {
		return err
	}
	return nil
}

func validateAbsoluteURI(name, raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" {
		return fmt.Errorf("Mill %s URI must be an absolute URI", name)
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("Mill %s URI must not contain a fragment", name)
	}
	return nil
}
