// This file describes the work assigned to an execution replica.
package execution

// ClaimedAttempt combines durable attempt identity with its workload input.
type ClaimedAttempt struct {
	Attempt        Attempt
	Executable     Executable
	ShardIndex     int
	InputURI       string
	InputStartByte int64
	InputEndByte   int64
	OutputURI      string
	Resources      Resources
}
