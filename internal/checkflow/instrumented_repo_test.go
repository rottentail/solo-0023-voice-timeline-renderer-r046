package checkflow

import (
	"testing"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/storage"
)

func TestInstrumentedRepositorySignalsCommitBoundaries(t *testing.T) {
	root := t.TempDir()
	physical, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	repo := newInstrumentedRepository(physical)
	signals := repo.arm()

	asset := domain.SourceAsset{
		ID:            "src_signal_1",
		Name:          "signalled",
		Digest:        "0123456789abcdef",
		ByteCount:     2,
		SampleRate:    48000,
		Channels:      1,
		BitsPerSample: 16,
		FrameCount:    1,
	}
	if err := repo.SaveSource(asset, []byte{1, 2}); err != nil {
		t.Fatalf("SaveSource: %v", err)
	}
	select {
	case <-signals.channel(TriggerSourceCommit):
	default:
		t.Fatal("source commit signal did not fire after SaveSource")
	}

	job := domain.RenderJob{ID: "job_signal_1", State: domain.RenderQueued}
	if err := repo.SaveJob(job); err != nil {
		t.Fatalf("SaveJob: %v", err)
	}
	select {
	case <-signals.channel(TriggerJobQueued):
	default:
		t.Fatal("job-queued signal did not fire after queued SaveJob")
	}

	receipt := domain.RenderReceipt{RenderID: "job_signal_1", Output: domain.OutputAsset{Digest: "abc"}}
	if err := repo.CommitRenderArtifacts("job_signal_1", []byte{9, 9}, receipt); err != nil {
		t.Fatalf("CommitRenderArtifacts: %v", err)
	}
	select {
	case <-signals.channel(TriggerRenderArtifacts):
	default:
		t.Fatal("artifact commit signal did not fire")
	}

	// After disarm, further commits must not close anything (the existing
	// channels are already closed, so validate against a fresh arming cycle).
	repo.disarm()
	fresh := repo.arm()
	if err := repo.SaveJob(domain.RenderJob{ID: "job_signal_2", State: domain.RenderRendering}); err != nil {
		t.Fatalf("SaveJob rendering: %v", err)
	}
	select {
	case <-fresh.channel(TriggerJobQueued):
		t.Fatal("a non-queued SaveJob must not fire the queued signal")
	default:
	}
}

func TestInstrumentedRepositoryForwardsReads(t *testing.T) {
	root := t.TempDir()
	physical, err := storage.NewRepository(root)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	repo := newInstrumentedRepository(physical)

	asset := domain.SourceAsset{ID: "src_read_1", Name: "readable", Digest: "aa", SampleRate: 48000, Channels: 1, BitsPerSample: 16, FrameCount: 1}
	if err := repo.SaveSource(asset, []byte{1}); err != nil {
		t.Fatalf("SaveSource: %v", err)
	}
	meta, err := repo.GetSourceMeta("src_read_1")
	if err != nil || meta.ID != "src_read_1" {
		t.Fatalf("GetSourceMeta not forwarded correctly: %+v err=%v", meta, err)
	}
	if _, err := repo.GetSourceMeta("missing"); err == nil {
		t.Fatal("expected not-found error to be forwarded unchanged")
	}
}
