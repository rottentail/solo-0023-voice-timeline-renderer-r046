package checkflow

import (
	"sync"

	"voice-timeline-renderer/internal/domain"
	"voice-timeline-renderer/internal/service"
)

// commitSignals holds the boundary signals for one interrupted attempt. Every
// channel is closed at most once, so consumers can wait without races.
type commitSignals struct {
	sourceCommit   chan struct{}
	jobQueued      chan struct{}
	artifactCommit chan struct{}
}

func newCommitSignals() *commitSignals {
	return &commitSignals{
		sourceCommit:   make(chan struct{}),
		jobQueued:      make(chan struct{}),
		artifactCommit: make(chan struct{}),
	}
}

func (signals *commitSignals) signal(trigger Trigger) {
	switch trigger {
	case TriggerSourceCommit:
		close(signals.sourceCommit)
	case TriggerJobQueued:
		close(signals.jobQueued)
	case TriggerRenderArtifacts:
		close(signals.artifactCommit)
	}
}

func (signals *commitSignals) channel(trigger Trigger) <-chan struct{} {
	switch trigger {
	case TriggerSourceCommit:
		return signals.sourceCommit
	case TriggerJobQueued:
		return signals.jobQueued
	case TriggerRenderArtifacts:
		return signals.artifactCommit
	default:
		return nil
	}
}

// instrumentedRepository wraps the production storage Repository without
// changing any of its semantics: it forwards every call unchanged and only
// closes boundary signals after successful commit calls return. The signals
// exist solely to anchor test fault injection at real disk-commit points.
type instrumentedRepository struct {
	inner service.Repository

	mu      sync.Mutex
	current *commitSignals
}

func newInstrumentedRepository(inner service.Repository) *instrumentedRepository {
	return &instrumentedRepository{inner: inner}
}

// arm installs fresh signals for the next interrupted attempt.
func (repo *instrumentedRepository) arm() *commitSignals {
	signals := newCommitSignals()
	repo.mu.Lock()
	repo.current = signals
	repo.mu.Unlock()
	return signals
}

// disarm detaches the signals so subsequent clean attempts cannot trigger cuts.
func (repo *instrumentedRepository) disarm() {
	repo.mu.Lock()
	repo.current = nil
	repo.mu.Unlock()
}

func (repo *instrumentedRepository) active() *commitSignals {
	repo.mu.Lock()
	defer repo.mu.Unlock()
	return repo.current
}

func (repo *instrumentedRepository) SaveSource(asset domain.SourceAsset, wav []byte) error {
	err := repo.inner.SaveSource(asset, wav)
	if err == nil {
		if signals := repo.active(); signals != nil {
			signals.signal(TriggerSourceCommit)
		}
	}
	return err
}

func (repo *instrumentedRepository) GetSourceMeta(id string) (domain.SourceAsset, error) {
	return repo.inner.GetSourceMeta(id)
}

func (repo *instrumentedRepository) GetSource(id string) (domain.SourceAsset, []byte, error) {
	return repo.inner.GetSource(id)
}

func (repo *instrumentedRepository) SaveTimeline(plan domain.Timeline) error {
	return repo.inner.SaveTimeline(plan)
}

func (repo *instrumentedRepository) GetTimeline(id string) (domain.Timeline, error) {
	return repo.inner.GetTimeline(id)
}

func (repo *instrumentedRepository) SaveJob(job domain.RenderJob) error {
	err := repo.inner.SaveJob(job)
	if err == nil {
		if signals := repo.active(); signals != nil {
			if job.State == domain.RenderQueued {
				signals.signal(TriggerJobQueued)
			}
		}
	}
	return err
}

func (repo *instrumentedRepository) GetJob(id string) (domain.RenderJob, error) {
	return repo.inner.GetJob(id)
}

func (repo *instrumentedRepository) CommitRenderArtifacts(id string, wav []byte, receipt domain.RenderReceipt) error {
	err := repo.inner.CommitRenderArtifacts(id, wav, receipt)
	if err == nil {
		if signals := repo.active(); signals != nil {
			signals.signal(TriggerRenderArtifacts)
		}
	}
	return err
}

func (repo *instrumentedRepository) GetRenderOutput(id string) ([]byte, domain.RenderReceipt, error) {
	return repo.inner.GetRenderOutput(id)
}
