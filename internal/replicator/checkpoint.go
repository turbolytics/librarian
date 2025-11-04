package replicator

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Checkpoint struct {
	ReplicatorID string    `json:"replicator_id"`
	Position     []byte    `json:"position"`
	Timestamp    time.Time `json:"timestamp"`
}

type Checkpointer interface {
	// Load the last checkpoint for a replicator
	Load(ctx context.Context, replicatorID string) (*Checkpoint, error)

	// Save a checkpoint
	Save(ctx context.Context, checkpoint *Checkpoint) error

	// Delete checkpoint data for a replicator
	Delete(ctx context.Context, replicatorID string) error
}

type NoopCheckpointer struct{}

func (n *NoopCheckpointer) Load(ctx context.Context, replicatorID string) (*Checkpoint, error) {
	return nil, nil
}
func (n *NoopCheckpointer) Save(ctx context.Context, checkpoint *Checkpoint) error {
	return nil
}
func (n *NoopCheckpointer) Delete(ctx context.Context, replicatorID string) error {
	return nil
}

// Filesystem-based checkpointer
type FilesystemCheckpointer struct {
	baseDir string
	logger  *zap.Logger
	mu      sync.Mutex
}

func NewFilesystemCheckpointer(baseDir string, logger *zap.Logger) *FilesystemCheckpointer {
	return &FilesystemCheckpointer{
		baseDir: baseDir,
		logger:  logger,
	}
}

func (f *FilesystemCheckpointer) Load(ctx context.Context, replicatorID string) (*Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	checkpointPath := filepath.Join(f.baseDir, replicatorID+".checkpoint")

	data, err := os.ReadFile(checkpointPath)
	if os.IsNotExist(err) {
		f.logger.Info("No checkpoint found", zap.String("replicator_id", replicatorID))
		return nil, nil // No checkpoint exists
	}
	if err != nil {
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	f.logger.Info("Checkpoint loaded",
		zap.String("replicator_id", replicatorID),
		zap.Time("timestamp", checkpoint.Timestamp),
	)

	return &checkpoint, nil
}

func (f *FilesystemCheckpointer) Save(ctx context.Context, checkpoint *Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure directory exists
	if err := os.MkdirAll(f.baseDir, 0755); err != nil {
		return err
	}

	checkpointPath := filepath.Join(f.baseDir, checkpoint.ReplicatorID+".checkpoint")
	tempPath := checkpointPath + ".tmp"

	// Marshal checkpoint data
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file first
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	// Sync to disk
	if file, err := os.OpenFile(tempPath, os.O_RDWR, 0644); err == nil {
		file.Sync()
		file.Close()
	}

	// Atomic rename
	if err := os.Rename(tempPath, checkpointPath); err != nil {
		os.Remove(tempPath) // Cleanup temp file
		return err
	}

	f.logger.Debug("Checkpoint saved",
		zap.String("replicator_id", checkpoint.ReplicatorID),
		zap.Time("timestamp", checkpoint.Timestamp),
	)

	return nil
}

func (f *FilesystemCheckpointer) Delete(ctx context.Context, replicatorID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	checkpointPath := filepath.Join(f.baseDir, replicatorID+".checkpoint")

	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	f.logger.Info("Checkpoint deleted", zap.String("replicator_id", replicatorID))
	return nil
}
