package checkflow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/httpapi"
	"voice-timeline-renderer/internal/service"
	"voice-timeline-renderer/internal/storage"
)

// attemptDeadline bounds every single proxy-mediated HTTP attempt. It is a
// liveness guard against a hung relay, never a synchronization mechanism: all
// ordering is driven by proxy stream events.
const attemptDeadline = 20 * time.Second

// AttemptKind classifies what the writing client actually observed.
const (
	ObservationComplete        = "complete_response"
	ObservationTransportError  = "transport_error"
	ObservationPartialResponse = "partial_response"
)

// AttemptObservation is the writer-side record of one request attempt.
type AttemptObservation struct {
	Role           string       `json:"role"`
	Method         string       `json:"method"`
	Path           string       `json:"path"`
	Fault          Fault        `json:"fault"`
	Observation    string       `json:"observation"`
	StatusCode     int          `json:"status_code"`
	TransportError string       `json:"transport_error,omitempty"`
	BodyReadError  string       `json:"body_read_error,omitempty"`
	BodyBytes      int          `json:"body_bytes"`
	Body           []byte       `json:"body,omitempty"`
	Events         []ProxyEvent `json:"events"`
}

// SourceDiskFact captures the on-disk truth for one source identifier.
type SourceDiskFact struct {
	Directory       string `json:"directory"`
	MetadataPresent bool   `json:"metadata_present"`
	WAVPresent      bool   `json:"wav_present"`
	WAVBytes        int64  `json:"wav_bytes"`
	WAVDigest       string `json:"wav_digest,omitempty"`
	MetadataDigest  string `json:"metadata_digest,omitempty"`
	DigestMatches   bool   `json:"digest_matches"`
	Metadata        string `json:"metadata,omitempty"`
}

// JobDiskFact captures the on-disk truth for one render identifier.
type JobDiskFact struct {
	JobFilePresent     bool   `json:"job_file_present"`
	State              string `json:"state"`
	ErrorCode          string `json:"error_code,omitempty"`
	ReceiptDigest      string `json:"receipt_digest,omitempty"`
	OutputPresent      bool   `json:"output_present"`
	ReceiptPresent     bool   `json:"receipt_present"`
	OutputBytes        int64  `json:"output_bytes"`
	OutputDigest       string `json:"output_digest,omitempty"`
	ReceiptDigestMatch bool   `json:"receipt_digest_match"`
	JobJSON            string `json:"job_json,omitempty"`
	ReceiptJSON        string `json:"receipt_json,omitempty"`
}

// DiskFacts is the full filesystem snapshot taken at reconciliation time.
type DiskFacts struct {
	Root          string                    `json:"root"`
	Sources       map[string]SourceDiskFact `json:"sources"`
	Jobs          map[string]JobDiskFact    `json:"jobs"`
	TempLeftovers []string                  `json:"temp_leftovers"`
	DirectoryTree string                    `json:"directory_tree"`
}

// DisconnectHarness wires the real HTTP service to fault-injecting proxies.
type DisconnectHarness struct {
	tempDir string
	server  *httptest.Server
	target  string
	repo    *instrumentedRepository
}

func newDisconnectHarness() (*DisconnectHarness, error) {
	tempDir, err := os.MkdirTemp("", "voice-disconnect-data-*")
	if err != nil {
		return nil, err
	}
	physical, err := storage.NewRepository(tempDir)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	repo := newInstrumentedRepository(physical)
	serviceLayer := service.New(repo, service.DefaultConfig())
	api := httpapi.New(serviceLayer, httpapi.Options{
		MaxSourceRequestBytes: 48 << 20,
		MaxJSONBytes:          2 << 20,
	})
	server := httptest.NewServer(api.Handler())
	server.Config.ErrorLog = discardLogger()
	return &DisconnectHarness{
		tempDir: tempDir,
		server:  server,
		target:  strings.TrimPrefix(server.URL, "http://"),
		repo:    repo,
	}, nil
}

// arm installs fresh commit-boundary signals for the next interrupted attempt.
func (h *DisconnectHarness) arm() *commitSignals {
	return h.repo.arm()
}

// disarm ensures subsequent clean attempts cannot trip any leftover cut.
func (h *DisconnectHarness) disarm() {
	h.repo.disarm()
}

func (h *DisconnectHarness) close(keepData bool) {
	h.server.Close()
	if !keepData {
		_ = os.RemoveAll(h.tempDir)
	}
}

// writerClient is used by the retrying writer; every attempt gets a fresh
// transport so no reused connection can mask a fault.
func newWriterClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

// newReaderClient deliberately constructs an independent HTTP client, modelling
// a separate observer that never saw the writer's interrupted connection.
func newReaderClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}
}

// attempt sends one request through a fresh fault proxy and records exactly
// what the client observed together with the proxy's network event log.
func (h *DisconnectHarness) attempt(
	parent context.Context,
	client *http.Client,
	role string,
	fault Fault,
	method, path string,
	body []byte,
) (AttemptObservation, error) {
	ctx, cancel := context.WithTimeout(parent, attemptDeadline)
	defer cancel()

	proxy, err := NewFaultProxy(h.target, fault)
	if err != nil {
		return AttemptObservation{}, err
	}
	defer proxy.Close()

	url := "http://" + proxy.Address() + path
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return AttemptObservation{}, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	observation := AttemptObservation{
		Role:   role,
		Method: method,
		Path:   path,
		Fault:  fault,
	}

	response, doErr := client.Do(request)
	if doErr != nil {
		observation.Observation = ObservationTransportError
		observation.TransportError = doErr.Error()
	} else {
		defer response.Body.Close()
		observation.StatusCode = response.StatusCode
		payload, readErr := io.ReadAll(response.Body)
		observation.Body = payload
		observation.BodyBytes = len(payload)
		if readErr != nil {
			observation.Observation = ObservationPartialResponse
			observation.BodyReadError = readErr.Error()
		} else {
			observation.Observation = ObservationComplete
		}
	}

	// Event-driven settle: wait until the service-side socket terminates so the
	// handler's storage effects are final before the disk snapshot is taken.
	select {
	case <-proxy.UpstreamDone():
	case <-ctx.Done():
		return observation, fmt.Errorf("service-side connection did not settle: %w", ctx.Err())
	}
	select {
	case <-proxy.ConnectionDone():
	case <-ctx.Done():
		return observation, fmt.Errorf("relay did not tear down: %w", ctx.Err())
	}
	observation.Events = proxy.Events()
	return observation, nil
}

// cleanAttempt issues a fault-free request on a fresh real TCP connection.
func (h *DisconnectHarness) cleanAttempt(ctx context.Context, client *http.Client, role, method, path string, body []byte) (AttemptObservation, error) {
	return h.attempt(ctx, client, role, Fault{ResponseMode: ResponseCutNone}, method, path, body)
}

// snapshotDisk reads the data directory and converts it into verifiable facts.
func (h *DisconnectHarness) snapshotDisk() (DiskFacts, error) {
	facts := DiskFacts{
		Root:    h.tempDir,
		Sources: map[string]SourceDiskFact{},
		Jobs:    map[string]JobDiskFact{},
	}

	sourceEntries, err := os.ReadDir(filepath.Join(h.tempDir, "sources"))
	if err != nil {
		return facts, err
	}
	for _, entry := range sourceEntries {
		if !entry.IsDir() {
			continue
		}
		fact := SourceDiskFact{Directory: filepath.Join("sources", entry.Name())}
		metaPath := filepath.Join(h.tempDir, "sources", entry.Name(), "metadata.json")
		if metaBytes, readErr := os.ReadFile(metaPath); readErr == nil {
			fact.MetadataPresent = true
			fact.Metadata = string(metaBytes)
			var asset domain.SourceAsset
			if json.Unmarshal(metaBytes, &asset) == nil {
				fact.MetadataDigest = asset.Digest
			}
		}
		wavPath := filepath.Join(h.tempDir, "sources", entry.Name(), "source.wav")
		if wavBytes, readErr := os.ReadFile(wavPath); readErr == nil {
			fact.WAVPresent = true
			fact.WAVBytes = int64(len(wavBytes))
			digest := sha256.Sum256(wavBytes)
			fact.WAVDigest = hex.EncodeToString(digest[:])
			fact.DigestMatches = fact.WAVDigest == fact.MetadataDigest
		}
		facts.Sources[entry.Name()] = fact
	}

	jobEntries, err := os.ReadDir(filepath.Join(h.tempDir, "jobs"))
	if err != nil {
		return facts, err
	}
	for _, entry := range jobEntries {
		if entry.IsDir() {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		jobPath := filepath.Join(h.tempDir, "jobs", entry.Name())
		jobBytes, readErr := os.ReadFile(jobPath)
		if readErr != nil {
			continue
		}
		fact := JobDiskFact{JobFilePresent: true, JobJSON: string(jobBytes)}
		var job domain.RenderJob
		if json.Unmarshal(jobBytes, &job) == nil {
			fact.State = string(job.State)
			if job.Error != nil {
				fact.ErrorCode = job.Error.Code
			}
			fact.ReceiptDigest = job.ReceiptDigest
		}
		renderDir := filepath.Join(h.tempDir, "renders", id)
		if outputBytes, readErr := os.ReadFile(filepath.Join(renderDir, "output.wav")); readErr == nil {
			fact.OutputPresent = true
			fact.OutputBytes = int64(len(outputBytes))
			digest := sha256.Sum256(outputBytes)
			fact.OutputDigest = hex.EncodeToString(digest[:])
		}
		if receiptBytes, readErr := os.ReadFile(filepath.Join(renderDir, "receipt.json")); readErr == nil {
			fact.ReceiptPresent = true
			fact.ReceiptJSON = string(receiptBytes)
			var receipt domain.RenderReceipt
			if json.Unmarshal(receiptBytes, &receipt) == nil {
				fact.ReceiptDigestMatch = receipt.Output.Digest == fact.OutputDigest &&
					receipt.Output.Digest == fact.ReceiptDigest
			}
		}
		facts.Jobs[id] = fact
	}

	if tmpEntries, err := os.ReadDir(filepath.Join(h.tempDir, "tmp")); err == nil {
		for _, entry := range tmpEntries {
			facts.TempLeftovers = append(facts.TempLeftovers, entry.Name())
		}
	}
	facts.DirectoryTree = renderTree(h.tempDir)
	return facts, nil
}

// renderTree produces a compact listing of every file with its size, the
// minimal disk layout evidence needed to explain a reconciliation failure.
func renderTree(root string) string {
	var builder strings.Builder
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(&builder, "%s (error: %v)\n", path, err)
			return nil
		}
		relative, _ := filepath.Rel(root, path)
		if info.IsDir() {
			fmt.Fprintf(&builder, "%s/\n", relative)
			return nil
		}
		fmt.Fprintf(&builder, "%s (%d bytes)\n", relative, info.Size())
		return nil
	})
	if err != nil {
		fmt.Fprintf(&builder, "walk error: %v\n", err)
	}
	return builder.String()
}

// discardLogger keeps httptest's expected client-disconnect log lines out of
// the workflow output; the meaningful network facts live in proxy events.
func discardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}
