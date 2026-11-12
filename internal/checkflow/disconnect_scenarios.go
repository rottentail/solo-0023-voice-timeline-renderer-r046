package checkflow

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/service"
)

// DisconnectScenarioResult aggregates every fact produced by one connection-
// interruption scenario. On failure the whole record is written to disk so the
// uncertain outcome can be replayed and explained.
type DisconnectScenarioResult struct {
	Name         string               `json:"name"`
	ResourceID   string               `json:"resource_id"`
	Setup        []AttemptObservation `json:"setup"`
	FirstAttempt AttemptObservation   `json:"first_attempt"`
	Retry        AttemptObservation   `json:"retry"`
	ReaderViews  []AttemptObservation `json:"reader_views"`
	DiskBefore   DiskFacts            `json:"disk_before_retry"`
	DiskAfter    DiskFacts            `json:"disk_after_retry"`
	Failures     []string             `json:"failures"`
}

// RunDisconnectReconciliation exercises real TCP connection interruption for
// source registration and render creation, then reconciles the writer's
// interrupted record, a same-identifier retry, an independent reader, and the
// on-disk facts. It never assumes up front which attempt "wins": the expected
// retry outcome is derived from the disk state after the interrupted attempt.
func RunDisconnectReconciliation(ctx context.Context) error {
	harness, err := newDisconnectHarness()
	if err != nil {
		return err
	}
	evidenceRoot, err := os.MkdirTemp("", "voice-disconnect-evidence-*")
	if err != nil {
		return err
	}

	results := make([]DisconnectScenarioResult, 0, 6)
	failed := false

	scenarios := []struct {
		name string
		run  func(context.Context, *DisconnectHarness) DisconnectScenarioResult
	}{
		{"source-request-truncated", runSourceRequestTruncated},
		{"source-response-before-any", runSourceResponseBeforeAny},
		{"source-response-partial-body", runSourceResponsePartial},
		{"render-request-truncated", runRenderRequestTruncated},
		{"render-response-before-any", runRenderResponseBeforeAny},
		{"render-response-partial-body", runRenderResponsePartial},
		{"render-cut-after-queued", runRenderCutAfterQueued},
	}
	for _, scenario := range scenarios {
		result := scenario.run(ctx, harness)
		results = append(results, result)
		if len(result.Failures) > 0 {
			failed = true
			if writeErr := writeScenarioEvidence(evidenceRoot, result); writeErr != nil {
				fmt.Fprintf(os.Stderr, "cannot write evidence for %s: %v\n", result.Name, writeErr)
			}
		}
	}

	if failed {
		harness.close(true)
		var summary strings.Builder
		fmt.Fprintf(&summary, "disconnect reconciliation failed; evidence retained at %s (data dir %s)\n", evidenceRoot, harness.tempDir)
		for _, result := range results {
			if len(result.Failures) == 0 {
				continue
			}
			fmt.Fprintf(&summary, "\n[%s]\n", result.Name)
			for _, failure := range result.Failures {
				fmt.Fprintf(&summary, "  - %s\n", failure)
			}
		}
		return fmt.Errorf("%s", summary.String())
	}

	harness.close(false)
	_ = os.RemoveAll(evidenceRoot)
	return nil
}

// DisconnectScenarioNames lists every supported scenario in run order.
func DisconnectScenarioNames() []string {
	return []string{
		"source-request-truncated",
		"source-response-before-any",
		"source-response-partial-body",
		"render-request-truncated",
		"render-response-before-any",
		"render-response-partial-body",
		"render-cut-after-queued",
	}
}

func scenarioRunner(name string) (func(context.Context, *DisconnectHarness) DisconnectScenarioResult, bool) {
	switch name {
	case "source-request-truncated":
		return runSourceRequestTruncated, true
	case "source-response-before-any":
		return runSourceResponseBeforeAny, true
	case "source-response-partial-body":
		return runSourceResponsePartial, true
	case "render-request-truncated":
		return runRenderRequestTruncated, true
	case "render-response-before-any":
		return runRenderResponseBeforeAny, true
	case "render-response-partial-body":
		return runRenderResponsePartial, true
	case "render-cut-after-queued":
		return runRenderCutAfterQueued, true
	default:
		return nil, false
	}
}

// RunSingleDisconnectScenario runs one scenario on an isolated data directory.
// On failure it retains a replayable evidence directory and returns its path in
// the error message; on success all temporary data is removed.
func RunSingleDisconnectScenario(ctx context.Context, name string) error {
	runner, ok := scenarioRunner(name)
	if !ok {
		return fmt.Errorf("unknown disconnect scenario %q", name)
	}
	harness, err := newDisconnectHarness()
	if err != nil {
		return err
	}
	evidenceRoot, err := os.MkdirTemp("", "voice-disconnect-evidence-*")
	if err != nil {
		return err
	}
	result := runner(ctx, harness)
	if len(result.Failures) == 0 {
		harness.close(false)
		_ = os.RemoveAll(evidenceRoot)
		return nil
	}
	harness.close(true)
	if writeErr := writeScenarioEvidence(evidenceRoot, result); writeErr != nil {
		return fmt.Errorf("scenario %s failed and evidence could not be written: %v (original failures: %v)", name, writeErr, result.Failures)
	}
	var summary strings.Builder
	fmt.Fprintf(&summary, "scenario %s failed; replayable evidence retained at %s (data dir %s)\n", name, evidenceRoot, harness.tempDir)
	for _, failure := range result.Failures {
		fmt.Fprintf(&summary, "  - %s\n", failure)
	}
	return fmt.Errorf("%s", summary.String())
}

// ---- source registration scenarios -------------------------------------------------

func runSourceRequestTruncated(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	wav, err := sineWAV(48000, 1, 200, 440, 0.4)
	if err != nil {
		return DisconnectScenarioResult{Name: "source-request-truncated", Failures: []string{err.Error()}}
	}
	return sourceScenario(ctx, harness, "source-request-truncated", "src_req_trunc_1", wav, Fault{
		Phase:            FaultRequest,
		RequestForwarded: 0, // set below once the request length is known
	})
}

func runSourceResponseBeforeAny(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	wav, err := sineWAV(48000, 1, 200, 440, 0.4)
	if err != nil {
		return DisconnectScenarioResult{Name: "source-response-before-any", Failures: []string{err.Error()}}
	}
	return sourceScenario(ctx, harness, "source-response-before-any", "src_resp_none_1", wav, Fault{
		Phase:        FaultResponse,
		ResponseMode: ResponseCutBeforeAny,
		ArmTrigger:   TriggerSourceCommit,
	})
}

func runSourceResponsePartial(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	wav, err := sineWAV(48000, 1, 200, 440, 0.4)
	if err != nil {
		return DisconnectScenarioResult{Name: "source-response-partial-body", Failures: []string{err.Error()}}
	}
	return sourceScenario(ctx, harness, "source-response-partial-body", "src_resp_part_1", wav, Fault{
		Phase:             FaultResponse,
		ResponseMode:      ResponseCutPartialBody,
		ResponseBodyBytes: 8,
		ArmTrigger:        TriggerSourceCommit,
	})
}

func sourceScenario(ctx context.Context, harness *DisconnectHarness, name, id string, wav []byte, fault Fault) DisconnectScenarioResult {
	result := DisconnectScenarioResult{Name: name, ResourceID: id}
	requestBody, err := json.Marshal(service.SourceWriteRequest{
		Name:      "disconnect source " + id,
		WAVBase64: base64.StdEncoding.EncodeToString(wav),
	})
	if err != nil {
		result.Failures = append(result.Failures, err.Error())
		return result
	}
	expectedDigest := sha256Hex(wav)
	if fault.Phase == FaultRequest {
		fault.RequestForwarded = len(requestBody) / 2
	}
	if fault.Phase == FaultResponse {
		signals := harness.arm()
		fault.ArmSignal = signals.channel(fault.ArmTrigger)
	}

	writer := newWriterClient()
	first, err := harness.attempt(ctx, writer, "writer", fault, "PUT", "/v1/sources/"+id, requestBody)
	harness.disarm()
	if err != nil {
		result.Failures = append(result.Failures, "interrupted attempt harness error: "+err.Error())
		return result
	}
	result.FirstAttempt = first
	if first.Observation == ObservationComplete {
		result.Failures = append(result.Failures, "fault injection did not interrupt the client: first attempt returned a complete response")
	}

	before, err := harness.snapshotDisk()
	if err != nil {
		result.Failures = append(result.Failures, "disk snapshot before retry failed: "+err.Error())
		return result
	}
	result.DiskBefore = before

	retry, err := harness.cleanAttempt(ctx, newWriterClient(), "writer-retry", "PUT", "/v1/sources/"+id, requestBody)
	if err != nil {
		result.Failures = append(result.Failures, "retry harness error: "+err.Error())
		return result
	}
	result.Retry = retry

	readerView, err := harness.cleanAttempt(ctx, newReaderClient(), "independent-reader", "GET", "/v1/sources/"+id, nil)
	if err != nil {
		result.Failures = append(result.Failures, "reader harness error: "+err.Error())
		return result
	}
	result.ReaderViews = []AttemptObservation{readerView}

	after, err := harness.snapshotDisk()
	if err != nil {
		result.Failures = append(result.Failures, "disk snapshot after retry failed: "+err.Error())
		return result
	}
	result.DiskAfter = after

	result.Failures = append(result.Failures, reconcileSource(id, expectedDigest, first, before, retry, readerView, after)...)
	return result
}

func reconcileSource(
	id, expectedDigest string,
	first AttemptObservation,
	before DiskFacts,
	retry AttemptObservation,
	reader AttemptObservation,
	after DiskFacts,
) []string {
	var failures []string

	if first.Observation == ObservationComplete {
		failures = append(failures, "interrupted attempt produced a complete response; network evidence cannot explain uncertainty")
	}

	beforeFact, beforePresent := before.Sources[id]
	if beforePresent {
		if !beforeFact.MetadataPresent || !beforeFact.WAVPresent || !beforeFact.DigestMatches {
			failures = append(failures, fmt.Sprintf("partially committed source visible after interrupted attempt: %+v", beforeFact))
		}
		if beforeFact.WAVDigest != expectedDigest {
			failures = append(failures, "committed source digest "+beforeFact.WAVDigest+" does not match the request payload digest "+expectedDigest)
		}
	}

	if retry.Observation != ObservationComplete {
		failures = append(failures, "same-id retry over a clean connection did not complete: "+retry.Observation+" "+retry.TransportError+retry.BodyReadError)
		return failures
	}

	// The acceptable retry status is derived from the disk facts, never
	// preselected by the test: 201 means nothing was committed before, 200 means
	// the interrupted attempt's commit survived and idempotency must replay it.
	var retryAsset domain.SourceAsset
	if decodeErr := json.Unmarshal(retry.Body, &retryAsset); decodeErr != nil {
		failures = append(failures, "retry response is not decodable source metadata: "+decodeErr.Error())
		return failures
	}
	switch {
	case !beforePresent && retry.StatusCode != 201:
		failures = append(failures, fmt.Sprintf("disk showed no source but retry returned status %d instead of 201", retry.StatusCode))
	case beforePresent && retry.StatusCode != 200:
		failures = append(failures, fmt.Sprintf("disk already held the source but retry returned status %d instead of the idempotent 200", retry.StatusCode))
	}
	if retryAsset.Digest != expectedDigest {
		failures = append(failures, "retry returned digest "+retryAsset.Digest+" but the payload digest is "+expectedDigest)
	}

	afterFact, afterPresent := after.Sources[id]
	if !afterPresent {
		failures = append(failures, "source is missing from disk after a successful retry")
	} else {
		if !afterFact.MetadataPresent || !afterFact.WAVPresent || !afterFact.DigestMatches {
			failures = append(failures, fmt.Sprintf("source on disk is internally inconsistent after retry: %+v", afterFact))
		}
		if afterFact.WAVDigest != expectedDigest || retryAsset.Digest != afterFact.WAVDigest {
			failures = append(failures, "writer retry, reader, and disk do not agree on one source digest")
		}
	}

	if reader.Observation != ObservationComplete {
		failures = append(failures, "independent reader could not complete its request: "+reader.Observation)
	} else {
		var readerAsset domain.SourceAsset
		if decodeErr := json.Unmarshal(reader.Body, &readerAsset); decodeErr != nil {
			failures = append(failures, "reader response is not decodable: "+decodeErr.Error())
		} else {
			if reader.StatusCode != 200 || readerAsset.Digest != expectedDigest {
				failures = append(failures, fmt.Sprintf("independent reader diverges: status=%d digest=%s want 200/%s", reader.StatusCode, readerAsset.Digest, expectedDigest))
			}
			if afterPresent && readerAsset.Digest != afterFact.WAVDigest {
				failures = append(failures, "reader digest does not match on-disk digest")
			}
		}
	}

	if len(after.TempLeftovers) > 0 {
		failures = append(failures, "staging leftovers in tmp/ break the atomic-commit guarantee: "+strings.Join(after.TempLeftovers, ", "))
	}
	return failures
}

// ---- render creation scenarios ------------------------------------------------------

func runRenderRequestTruncated(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	return renderScenario(ctx, harness, "render-request-truncated", "rnd_req_trunc_1", Fault{
		Phase:            FaultRequest,
		RequestForwarded: 0,
	})
}

func runRenderResponseBeforeAny(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	return renderScenario(ctx, harness, "render-response-before-any", "rnd_resp_none_1", Fault{
		Phase:        FaultResponse,
		ResponseMode: ResponseCutBeforeAny,
		ArmTrigger:   TriggerRenderArtifacts,
	})
}

func runRenderResponsePartial(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	return renderScenario(ctx, harness, "render-response-partial-body", "rnd_resp_part_1", Fault{
		Phase:             FaultResponse,
		ResponseMode:      ResponseCutPartialBody,
		ResponseBodyBytes: 8,
		ArmTrigger:        TriggerRenderArtifacts,
	})
}

// runRenderCutAfterQueued cuts the connection at the earliest durable render
// boundary: the queued job has been persisted but rendering has not finished.
// This is the genuinely ambiguous window the success promise must survive: a
// retry must resume against the same identifier rather than losing the job.
func runRenderCutAfterQueued(ctx context.Context, harness *DisconnectHarness) DisconnectScenarioResult {
	return renderScenario(ctx, harness, "render-cut-after-queued", "rnd_queued_cut_1", Fault{
		Phase:        FaultResponse,
		ResponseMode: ResponseCutBeforeAny,
		ArmTrigger:   TriggerJobQueued,
	})
}

func renderScenario(ctx context.Context, harness *DisconnectHarness, name, renderID string, fault Fault) DisconnectScenarioResult {
	result := DisconnectScenarioResult{Name: name, ResourceID: renderID}
	timelineID, setup, setupFailures := setupRenderTimeline(ctx, harness, renderID)
	result.Setup = setup
	if len(setupFailures) > 0 {
		result.Failures = append(result.Failures, setupFailures...)
		return result
	}

	requestBody, err := json.Marshal(service.RenderWriteRequest{TimelineID: timelineID})
	if err != nil {
		result.Failures = append(result.Failures, err.Error())
		return result
	}
	if fault.Phase == FaultRequest {
		fault.RequestForwarded = len(requestBody) / 2
	}
	if fault.Phase == FaultResponse {
		signals := harness.arm()
		fault.ArmSignal = signals.channel(fault.ArmTrigger)
	}

	first, err := harness.attempt(ctx, newWriterClient(), "writer", fault, "PUT", "/v1/renders/"+renderID, requestBody)
	harness.disarm()
	if err != nil {
		result.Failures = append(result.Failures, "interrupted attempt harness error: "+err.Error())
		return result
	}
	result.FirstAttempt = first
	if first.Observation == ObservationComplete {
		result.Failures = append(result.Failures, "fault injection did not interrupt the client: first attempt returned a complete response")
	}

	before, err := harness.snapshotDisk()
	if err != nil {
		result.Failures = append(result.Failures, "disk snapshot before retry failed: "+err.Error())
		return result
	}
	result.DiskBefore = before

	retry, err := harness.cleanAttempt(ctx, newWriterClient(), "writer-retry", "PUT", "/v1/renders/"+renderID, requestBody)
	if err != nil {
		result.Failures = append(result.Failures, "retry harness error: "+err.Error())
		return result
	}
	result.Retry = retry

	readerJob, err := harness.cleanAttempt(ctx, newReaderClient(), "independent-reader", "GET", "/v1/renders/"+renderID, nil)
	if err != nil {
		result.Failures = append(result.Failures, "reader job harness error: "+err.Error())
		return result
	}
	readerReceipt, err := harness.cleanAttempt(ctx, newReaderClient(), "independent-reader", "GET", "/v1/renders/"+renderID+"/receipt", nil)
	if err != nil {
		result.Failures = append(result.Failures, "reader receipt harness error: "+err.Error())
		return result
	}
	readerOutput, err := harness.cleanAttempt(ctx, newReaderClient(), "independent-reader", "GET", "/v1/renders/"+renderID+"/output", nil)
	if err != nil {
		result.Failures = append(result.Failures, "reader output harness error: "+err.Error())
		return result
	}
	result.ReaderViews = []AttemptObservation{readerJob, readerReceipt, readerOutput}

	after, err := harness.snapshotDisk()
	if err != nil {
		result.Failures = append(result.Failures, "disk snapshot after retry failed: "+err.Error())
		return result
	}
	result.DiskAfter = after

	result.Failures = append(result.Failures, reconcileRender(renderID, first, before, retry, readerJob, readerReceipt, readerOutput, after)...)
	return result
}

// setupRenderTimeline creates uniquely named sources and a timeline using only
// clean real-TCP requests, returning the ready timeline identifier.
func setupRenderTimeline(ctx context.Context, harness *DisconnectHarness, renderID string) (string, []AttemptObservation, []string) {
	var observations []AttemptObservation
	var failures []string
	firstWAV, err := sineWAV(48000, 1, 300, 220, 0.25)
	if err != nil {
		return "", nil, []string{err.Error()}
	}
	secondWAV, err := sineWAV(44100, 2, 250, 330, 0.2)
	if err != nil {
		return "", nil, []string{err.Error()}
	}
	sourceA := renderID + "_src_a"
	sourceB := renderID + "_src_b"
	timelineID := renderID + "_timeline"

	bodyA, _ := json.Marshal(service.SourceWriteRequest{Name: sourceA, WAVBase64: base64.StdEncoding.EncodeToString(firstWAV)})
	obs, err := harness.cleanAttempt(ctx, newWriterClient(), "setup", "PUT", "/v1/sources/"+sourceA, bodyA)
	if err != nil {
		return "", nil, []string{"setup source A harness error: " + err.Error()}
	}
	observations = append(observations, obs)
	if obs.StatusCode != 201 {
		failures = append(failures, fmt.Sprintf("setup source A returned status %d", obs.StatusCode))
	}

	bodyB, _ := json.Marshal(service.SourceWriteRequest{Name: sourceB, WAVBase64: base64.StdEncoding.EncodeToString(secondWAV)})
	obs, err = harness.cleanAttempt(ctx, newWriterClient(), "setup", "PUT", "/v1/sources/"+sourceB, bodyB)
	if err != nil {
		return "", nil, []string{"setup source B harness error: " + err.Error()}
	}
	observations = append(observations, obs)
	if obs.StatusCode != 201 {
		failures = append(failures, fmt.Sprintf("setup source B returned status %d", obs.StatusCode))
	}

	spans := []domain.TimelineSpan{
		{SourceID: sourceA, SourceStartFrame: 0, SourceEndFrame: 14400, GainDB: -2, CrossfadeFrames: 0},
		{SourceID: sourceB, SourceStartFrame: 0, SourceEndFrame: 11025, GainDB: 0, CrossfadeFrames: 2400},
	}
	timelineBody, _ := json.Marshal(service.TimelineWriteRequest{
		Name:           "disconnect timeline " + timelineID,
		SampleRate:     48000,
		OutputChannels: 2,
		TargetPeakDBFS: -1,
		Spans:          spans,
	})
	obs, err = harness.cleanAttempt(ctx, newWriterClient(), "setup", "PUT", "/v1/timelines/"+timelineID, timelineBody)
	if err != nil {
		return "", nil, []string{"setup timeline harness error: " + err.Error()}
	}
	observations = append(observations, obs)
	if obs.StatusCode != 201 {
		failures = append(failures, fmt.Sprintf("setup timeline returned status %d", obs.StatusCode))
	}
	return timelineID, observations, failures
}

func reconcileRender(
	id string,
	first AttemptObservation,
	before DiskFacts,
	retry AttemptObservation,
	readerJob, readerReceipt, readerOutput AttemptObservation,
	after DiskFacts,
) []string {
	var failures []string

	if first.Observation == ObservationComplete {
		failures = append(failures, "interrupted attempt produced a complete response; network evidence cannot explain uncertainty")
	}

	beforeFact, beforePresent := before.Jobs[id]
	if beforePresent {
		switch beforeFact.State {
		case string(domain.RenderQueued), string(domain.RenderRendering), string(domain.RenderFinalizing):
			failures = append(failures, fmt.Sprintf("job persisted in non-terminal state %q after the service-side connection ended; the success promise is undecidable", beforeFact.State))
		case string(domain.RenderFailed):
			// The proxy events prove the cut happened while the response was in
			// flight (or before it), so a terminal failure cannot be blamed on
			// the client disconnect once the handler drained to EOF.
			failures = append(failures, fmt.Sprintf("disconnect was recorded as business failure %q; network and disk evidence cannot explain why the valid render failed", beforeFact.ErrorCode))
		case string(domain.RenderCompleted):
			if !beforeFact.OutputPresent || !beforeFact.ReceiptPresent || !beforeFact.ReceiptDigestMatch {
				failures = append(failures, fmt.Sprintf("completed job lacks self-consistent artifacts after interrupted attempt: %+v", beforeFact))
			}
		default:
			failures = append(failures, "job persisted with unknown state "+beforeFact.State)
		}
	}

	if retry.Observation != ObservationComplete {
		failures = append(failures, "same-id render retry over a clean connection did not complete: "+retry.Observation+" "+retry.TransportError+retry.BodyReadError)
		return failures
	}
	var retryJob domain.RenderJob
	if decodeErr := json.Unmarshal(retry.Body, &retryJob); decodeErr != nil {
		failures = append(failures, "retry response is not a decodable render job: "+decodeErr.Error())
		return failures
	}

	// Again, the acceptable retry answer is derived from the disk state rather
	// than chosen by the test.
	switch {
	case !beforePresent && retry.StatusCode != 201:
		failures = append(failures, fmt.Sprintf("disk showed no job but retry returned status %d instead of 201", retry.StatusCode))
	case beforePresent && retry.StatusCode != 200:
		failures = append(failures, fmt.Sprintf("disk already held the job but retry returned status %d instead of the idempotent 200", retry.StatusCode))
	}
	if retryJob.State != domain.RenderCompleted {
		failures = append(failures, "retry settled in state "+string(retryJob.State)+"; a valid timeline must reach completed")
		return failures
	}

	afterFact, afterPresent := after.Jobs[id]
	if !afterPresent {
		failures = append(failures, "render job missing from disk after successful retry")
		return failures
	}
	if afterFact.State != string(domain.RenderCompleted) || !afterFact.OutputPresent || !afterFact.ReceiptPresent || !afterFact.ReceiptDigestMatch {
		failures = append(failures, fmt.Sprintf("post-retry disk facts do not show a self-consistent completed render: %+v", afterFact))
	}
	if beforePresent && beforeFact.State == string(domain.RenderCompleted) {
		if beforeFact.OutputDigest != afterFact.OutputDigest {
			failures = append(failures, "retry produced a different output digest than the interrupted attempt's committed output: the same identifier now names two facts")
		}
		if retryJob.ReceiptDigest != afterFact.ReceiptDigest {
			failures = append(failures, "retry response digest diverges from the on-disk receipt digest")
		}
	}

	if readerJob.Observation != ObservationComplete || readerJob.StatusCode != 200 {
		failures = append(failures, fmt.Sprintf("independent reader could not read the job: observation=%s status=%d err=%s", readerJob.Observation, readerJob.StatusCode, readerJob.TransportError))
	} else {
		var readerJobBody domain.RenderJob
		if decodeErr := json.Unmarshal(readerJob.Body, &readerJobBody); decodeErr != nil {
			failures = append(failures, "reader job body not decodable: "+decodeErr.Error())
		} else if readerJobBody.State != domain.RenderCompleted || readerJobBody.ReceiptDigest != afterFact.ReceiptDigest {
			failures = append(failures, "reader's view of the job state or digest diverges from disk")
		}
	}
	if readerReceipt.Observation != ObservationComplete || readerReceipt.StatusCode != 200 {
		failures = append(failures, fmt.Sprintf("independent reader could not read the receipt: observation=%s status=%d", readerReceipt.Observation, readerReceipt.StatusCode))
	}
	if readerOutput.Observation != ObservationComplete || readerOutput.StatusCode != 200 {
		failures = append(failures, fmt.Sprintf("independent reader could not read the output: observation=%s status=%d err=%s", readerOutput.Observation, readerOutput.StatusCode, readerOutput.TransportError))
	} else if sha256Hex(readerOutput.Body) != afterFact.OutputDigest {
		failures = append(failures, "reader-downloaded output digest does not match the on-disk output digest")
	}

	if len(after.TempLeftovers) > 0 {
		failures = append(failures, "staging leftovers in tmp/ break the atomic-commit guarantee: "+strings.Join(after.TempLeftovers, ", "))
	}
	return failures
}

// ---- evidence -----------------------------------------------------------------------

func writeScenarioEvidence(root string, result DisconnectScenarioResult) error {
	dir := filepath.Join(root, result.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "scenario.json"), payload, 0o640); err != nil {
		return err
	}
	var builder strings.Builder
	builder.WriteString("## first-attempt network events (minimum facts explaining the cut)\n")
	for _, event := range result.FirstAttempt.Events {
		fmt.Fprintf(&builder, "%s\n", event)
	}
	builder.WriteString("\n## retry network events\n")
	for _, event := range result.Retry.Events {
		fmt.Fprintf(&builder, "%s\n", event)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.txt"), []byte(builder.String()), 0o640); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "disk-before-retry.txt"), []byte(result.DiskBefore.DirectoryTree), 0o640); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "disk-after-retry.txt"), []byte(result.DiskAfter.DirectoryTree), 0o640); err != nil {
		return err
	}
	var failureText strings.Builder
	for _, failure := range result.Failures {
		fmt.Fprintf(&failureText, "- %s\n", failure)
	}
	if err := os.WriteFile(filepath.Join(dir, "failures.txt"), []byte(failureText.String()), 0o640); err != nil {
		return err
	}
	return nil
}

func sha256Hex(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}
