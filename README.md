# Voice Timeline Renderer

Voice Timeline Renderer is a self-hosted Go service for turning validated PCM
WAV recordings into a crossfaded, peak-normalized mixdown. The service exposes
an idempotent HTTP surface, keeps media and metadata under one data directory,
and records every render transition.

The implementation covers:

- 16-bit integer PCM WAV validation and signal measurement.
- Ordered timeline spans with source-frame selection, gain, and crossfade.
- Linear sample-rate conversion and mono/stereo channel mapping.
- Equal-power neighboring-span crossfades.
- Peak normalization into a requested dBFS target.
- Durable render states, receipts, and atomically committed output folders.

Automated tests are intentionally deferred. A later task stage will add unit,
integration, and end-to-end verification around the production interfaces.

## Requirements

- Go 1.23 or newer.
- A writable local directory for source media, timeline metadata, render jobs,
  output WAV files, and receipts.

No network service, database server, or third-party Go module is required.

## Build

```bash
go build ./...
```

## Run

```bash
go run ./cmd/voiceservice \
  -addr :8093 \
  -data ./var/voice-timeline
```

The health endpoint is `GET http://127.0.0.1:8093/healthz`.

Runtime settings are supplied through arguments:

- `-addr`: HTTP listen address, default `:8093`.
- `-data`: persistent data root, default `./var/voice-timeline`.

No environment variable is required. Do not put credentials in the repository;
the service does not consume any remote credential.

## HTTP interface

### Register a source

`PUT /v1/sources/{source_id}`

```json
{
  "name": "opening take",
  "wav_base64": "UklGR..."
}
```

The source ID is chosen by the caller and must be 3-64 characters using
letters, digits, underscore, or hyphen. Repeating the same ID with exactly the
same name and bytes is idempotent. Reusing it with different content returns
the `source_conflict` error.

Response:

```json
{
  "id": "opening_take_01",
  "name": "opening take",
  "digest": "b7c0...",
  "byte_count": 96044,
  "sample_rate": 48000,
  "channels": 1,
  "bits_per_sample": 16,
  "frame_count": 48000,
  "duration_ms": 1000,
  "peak_dbfs": -8.0,
  "rms_dbfs": -11.0,
  "created_at": "2026-11-09T12:00:00Z"
}
```

### Compose a timeline

`PUT /v1/timelines/{timeline_id}`

```json
{
  "name": "two part narration",
  "sample_rate": 48000,
  "output_channels": 2,
  "target_peak_dbfs": -1,
  "spans": [
    {
      "source_id": "opening_take_01",
      "source_start_frame": 0,
      "source_end_frame": 48000,
      "gain_db": -2,
      "crossfade_frames": 0
    },
    {
      "source_id": "closing_take_01",
      "source_start_frame": 0,
      "source_end_frame": 44100,
      "gain_db": 0,
      "crossfade_frames": 4800
    }
  ]
}
```

Spans are sequential. A later span overlaps its predecessor by
`crossfade_frames`; the overlap is mixed with equal-power weights. A crossfade
must fit within both neighboring spans.

### Start a render

`PUT /v1/renders/{render_id}`

```json
{
  "timeline_id": "two_part_narration_01"
}
```

The render runs through the durable states
`queued -> rendering -> finalizing -> completed`. A failed render records a
terminal `failed` state with a stable error code and does not expose a partial
output folder.

### Read resources

- `GET /v1/sources/{source_id}`
- `GET /v1/timelines/{timeline_id}`
- `GET /v1/renders/{render_id}`
- `GET /v1/renders/{render_id}/receipt`
- `GET /v1/renders/{render_id}/output`

The output route returns `audio/wav`. The receipt records source digests, span
placement, channel mapping, measured peak and RMS amplitude, normalization
gain, output digest, and non-fatal warnings.

## Data directory

```text
var/voice-timeline/
  sources/<source_id>/
    source.wav
    metadata.json
  timelines/<timeline_id>.json
  jobs/<render_id>.json
  renders/<render_id>/
    output.wav
    receipt.json
  tmp/
```

Source and render folders are assembled in `tmp/` and moved into place with one
directory rename. Metadata updates use same-directory temporary files followed
by rename.

## Source layout

```text
cmd/voiceservice/       service entry point
cmd/timelinecheck/      workflow execution entry point
internal/httpapi/       routing, request limits, JSON errors, media response
internal/service/       workflow orchestration and idempotency
internal/domain/        entities, validation, errors, and render transitions
internal/audiocodec/    WAV handling, transforms, and signal measurement
internal/render/        timeline preparation, mixing, normalization, receipt
internal/storage/       atomic local repository
internal/checkflow/     bounded production workflow checks and disconnect reconciliation
  faultproxy.go         byte- and commit-anchored TCP connection fault injector
  instrumented_repo.go  test-only repository decorator emitting commit-boundary signals
  disconnect*.go        three-view reconciliation, disk facts, and replayable evidence
  *_test.go             unit tests for the proxy, decorator, and reconciliation
```

## Workflow checks

Run each declared production check from the repository root:

```bash
go run ./cmd/timelinecheck -workflow ingest-audio-source
go run ./cmd/timelinecheck -workflow compose-timeline
go run ./cmd/timelinecheck -workflow render-mixdown
go run ./cmd/timelinecheck -workflow disconnect-reconciliation
```

Each check creates an isolated temporary data directory, starts the real HTTP
handler, sends generated WAV inputs, and verifies read-back or rendered output.
The checks are bounded and do not require network access.

## Connection interruption reconciliation

The three classic workflow checks wait for a complete HTTP response, so they
cannot say what happens when the client receives only a partial response or no
response at all. `disconnect-reconciliation` closes that gap with a real
fault-injecting TCP relay (`internal/checkflow/faultproxy.go`) placed in front
of the unmodified HTTP service:

- **Request truncation** is anchored to relayed byte counts, so the service
  observes a genuinely truncated upload.
- **Response cuts** are armed by deterministic storage-commit boundary signals
  emitted by a test-only repository decorator
  (`internal/checkflow/instrumented_repo.go`): the cut fires only after the
  source folder or render artifacts have been atomically renamed into place (or
  after the durable queued job is written). No wall-clock sleep is used; the
  harness waits on proxy stream and upstream-EOF events.

For every interrupted write of a source or a render, the check reconciles
three independent views against the on-disk facts:

1. the writing client's record (transport error or partial response — a
   disconnection is never pre-classified as a business failure),
2. a retry with the same identifier over a clean connection, whose expected
   status (`201` created vs. `200` idempotent replay) is derived from the disk
   state rather than chosen by the test,
3. a separate reader client that never saw the interrupted connection.

A failure leaves a replayable evidence directory containing the minimum network
event log and disk layout needed to explain the uncertain outcome; the data
directory is retained and its path is printed.

### Known counter-example (render cut after the queued job)

Cutting the connection at the durable `queued` boundary exposes a current
limitation: the render pipeline observes request-context cancellation
(`internal/render/mixer.go`) and the service records it as a terminal
`render_failed` job. Because `completed` and `failed` are terminal, a same-ID
retry then returns the poisoned failed job forever instead of completing. The
counter-example is preserved by
`TestDisconnectReconciliationQueuedCutFailure` and must keep producing
replayable evidence until the service stops treating client disconnection as a
business failure. The other six interruption windows reconcile cleanly.

## Test boundary

Focused Go tests now cover the fault-injecting proxy, the commit-boundary
repository decorator, and the connection-interruption reconciliation
(`internal/checkflow/*_test.go`). Run them with:

```bash
go test ./...
```

Further coverage can extend around WAV chunk edge cases, timeline crossfade
bounds, failed job cleanup, and HTTP error mapping. The reconciliation tests
own the success-promise question under real socket interruption, including the
retained render-queued counter-example described above.
