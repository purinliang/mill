package executionrpc

// MaxMessageBytes bounds the execution control-plane payload on both sides of
// the connection. Attempt instructions contain metadata, never dataset bytes.
const MaxMessageBytes = 1 << 20
