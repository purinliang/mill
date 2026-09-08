package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
)

func TestJobServiceProcessServesJobsAndShutsDownGracefully(t *testing.T) {
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}

	binary := filepath.Join(t.TempDir(), "mill")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Job service: %v\n%s", err, output)
	}

	address := reserveLoopbackAddress(t)
	fixtureDirectory := t.TempDir()
	inputFilename := filepath.Join(fixtureDirectory, "input.jsonl")
	if err := os.WriteFile(inputFilename, []byte("{\"record\":1}\n{\"record\":2}\n{\"record\":3}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputURI := (&url.URL{Scheme: "file", Path: inputFilename}).String()
	outputRootURI := (&url.URL{Scheme: "file", Path: filepath.Join(fixtureDirectory, "output")}).String()
	idempotencyKey := fmt.Sprintf("process-public-api-%d", time.Now().UnixNano())
	cleanupProcessFixture(t, databaseURL, idempotencyKey)

	logFile, err := os.Create(filepath.Join(fixtureDirectory, "mill.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()

	process := exec.Command(binary)
	process.Env = environmentWithOverrides(map[string]string{
		"MILL_DATABASE_URL":    databaseURL,
		"MILL_OUTPUT_ROOT_URI": outputRootURI,
		"MILL_PARALLELISM":     "2",
		"MILL_HTTP_ADDR":       address,
		"MILL_GRPC_ADDR":       "",
		"AWS_REGION":           "",
		"MILL_S3_ENDPOINT":     "",
	})
	process.Stdout = logFile
	process.Stderr = logFile
	if err := process.Start(); err != nil {
		t.Fatalf("start Job service: %v", err)
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- process.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = process.Process.Kill()
			<-waitResult
		}
	})

	client := &http.Client{Timeout: 500 * time.Millisecond}
	waitForReady(t, client, address, waitResult, logFile, &exited)
	assertHealthResponse(t, client, "http://"+address+"/livez", "ok")
	assertHealthResponse(t, client, "http://"+address+"/readyz", "ready")

	submission := fmt.Sprintf(`{"executable":{"image":"mill/jsonl-copy:dev"},"input":{"uri":%q}}`, inputURI)
	created, location := submitProcessJob(t, client, address, idempotencyKey, submission, http.StatusCreated)
	if created.State != job.StateRunning || created.Parallelism != 2 || created.Progress != (job.Progress{Total: 3, Pending: 3}) {
		t.Fatalf("created job = state %q parallelism %d progress %+v", created.State, created.Parallelism, created.Progress)
	}
	if location != "/jobs/"+created.ID {
		t.Fatalf("Location = %q, want /jobs/%s", location, created.ID)
	}

	replayed, _ := submitProcessJob(t, client, address, idempotencyKey, submission, http.StatusOK)
	if replayed.ID != created.ID {
		t.Fatalf("replayed job ID = %q, want %q", replayed.ID, created.ID)
	}
	status := getProcessJob(t, client, address, created.ID)
	if status.ID != created.ID || status.Progress != created.Progress {
		t.Fatalf("job status = %+v", status)
	}

	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal Job service: %v", err)
	}
	select {
	case err := <-waitResult:
		exited = true
		if err != nil {
			t.Fatalf("Job service shutdown: %v; logs:\n%s", err, readProcessLog(logFile))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Job service did not stop after SIGTERM")
	}
}

func reserveLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForReady(t *testing.T, client *http.Client, address string, waitResult <-chan error, logFile *os.File, exited *bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-waitResult:
			*exited = true
			t.Fatalf("Job service exited before readiness: %v; logs:\n%s", err, readProcessLog(logFile))
		default:
		}
		response, err := client.Get("http://" + address + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Job service did not become ready; logs:\n%s", readProcessLog(logFile))
}

func assertHealthResponse(t *testing.T, client *http.Client, endpoint, expected string) {
	t.Helper()
	response, err := client.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || body.Status != expected {
		t.Fatalf("GET %s = status %d body %+v", endpoint, response.StatusCode, body)
	}
}

func submitProcessJob(t *testing.T, client *http.Client, address, key, submission string, expectedStatus int) (job.Job, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, "http://"+address+"/jobs", bytes.NewBufferString(submission))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("POST /jobs status = %d, want %d; body = %s", response.StatusCode, expectedStatus, body)
	}
	var created job.Job
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	return created, response.Header.Get("Location")
}

func getProcessJob(t *testing.T, client *http.Client, address, id string) job.Job {
	t.Helper()
	response, err := client.Get("http://" + address + "/jobs/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET /jobs/%s status = %d; body = %s", id, response.StatusCode, body)
	}
	var result job.Job
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cleanupProcessFixture(t *testing.T, databaseURL, idempotencyKey string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM public.jobs WHERE idempotency_key = $1", idempotencyKey); err != nil {
			t.Errorf("clean process fixture: %v", err)
		}
	})
}

func environmentWithOverrides(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replaced := overrides[name]; !replaced {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func readProcessLog(logFile *os.File) string {
	if _, err := logFile.Seek(0, io.SeekStart); err != nil {
		return "<cannot seek log: " + err.Error() + ">"
	}
	contents, err := io.ReadAll(logFile)
	if err != nil {
		return "<cannot read log: " + err.Error() + ">"
	}
	return string(contents)
}
