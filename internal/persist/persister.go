// Package persist provides state persistence for runtime statistics and metrics.
// It saves usage statistics, token metrics, and cooldown state to local files
// periodically and on graceful shutdown.
package persist

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	defaultPersistInterval = 60 * time.Second
	stateDirName           = "state"
	filePerm               = 0o600
)

// StatePersister manages periodic persistence of runtime state.
type StatePersister struct {
	mu        sync.RWMutex
	interval  time.Duration
	stateDir  string
	stopCh    chan struct{}
	wg        sync.WaitGroup
	providers []StateProvider
	started   atomic.Bool
}

// StateProvider is the interface that state holders implement to support persistence.
type StateProvider interface {
	// Name returns a unique identifier for the state file (without extension).
	Name() string
	// Save returns the state to persist.
	Save() (any, error)
	// Load restores state from persisted data.
	Load(data any) error
}

// stateFile wraps persisted data with metadata.
type stateFile struct {
	Version int       `json:"version"`
	SavedAt time.Time `json:"saved_at"`
	Data    any       `json:"data"`
}

// NewStatePersister creates a new state persister.
func NewStatePersister(stateDir string, interval time.Duration) *StatePersister {
	if interval <= 0 {
		interval = defaultPersistInterval
	}
	return &StatePersister{
		interval: interval,
		stateDir: stateDir,
		stopCh:   make(chan struct{}),
	}
}

// RegisterProvider adds a state provider to be persisted.
func (p *StatePersister) RegisterProvider(provider StateProvider) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.providers = append(p.providers, provider)
}

// Start begins the periodic persistence goroutine.
// Start must not be called more than once per StatePersister instance.
func (p *StatePersister) Start() {
	if !p.started.CompareAndSwap(false, true) {
		return // Already started
	}
	p.wg.Add(1)
	go p.run()
	log.WithField("dir", p.stateDir).WithField("interval", p.interval).Info("state persister started")
}

// Stop gracefully stops the persister and performs a final save.
func (p *StatePersister) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	// Final save on shutdown
	if err := p.SaveAll(); err != nil {
		log.WithError(err).Warn("final state save failed")
	}
	log.Info("state persister stopped")
}

// run is the main persistence loop.
func (p *StatePersister) run() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.SaveAll(); err != nil {
				log.WithError(err).Warn("periodic state save failed")
			}
		case <-p.stopCh:
			return
		}
	}
}

// SaveAll persists all registered providers to disk.
func (p *StatePersister) SaveAll() error {
	p.mu.RLock()
	providers := make([]StateProvider, len(p.providers))
	copy(providers, p.providers)
	p.mu.RUnlock()

	if len(providers) == 0 {
		return nil
	}

	// Ensure state directory exists
	if err := os.MkdirAll(p.stateDir, 0o700); err != nil {
		return fmt.Errorf("create state directory %s: %w", p.stateDir, err)
	}

	var lastErr error
	for _, provider := range providers {
		if err := p.saveProvider(provider); err != nil {
			log.WithError(err).WithField("provider", provider.Name()).Warn("failed to save state")
			lastErr = err
		}
	}
	return lastErr
}

// saveProvider persists a single provider's state.
func (p *StatePersister) saveProvider(provider StateProvider) error {
	data, err := provider.Save()
	if err != nil {
		return fmt.Errorf("save provider %s: %w", provider.Name(), err)
	}
	if data == nil {
		return nil
	}

	state := stateFile{
		Version: 1,
		SavedAt: time.Now().UTC(),
		Data:    data,
	}

	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state for %s: %w", provider.Name(), err)
	}

	filename := provider.Name() + ".json"
	path := filepath.Join(p.stateDir, filename)
	tmpPath := path + ".tmp"

	// Write to temp file first, then rename for atomicity
	if err := os.WriteFile(tmpPath, raw, filePerm); err != nil {
		return fmt.Errorf("write temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath) // Best effort cleanup
		return fmt.Errorf("rename state file %s: %w", path, err)
	}
	return nil
}

// LoadAll restores all registered providers from disk.
func (p *StatePersister) LoadAll() error {
	p.mu.RLock()
	providers := make([]StateProvider, len(p.providers))
	copy(providers, p.providers)
	p.mu.RUnlock()

	if len(providers) == 0 {
		return nil
	}

	for _, provider := range providers {
		if err := p.loadProvider(provider); err != nil {
			if !os.IsNotExist(err) {
				log.WithError(err).WithField("provider", provider.Name()).Warn("failed to load state")
			}
		}
	}
	return nil
}

// loadProvider restores a single provider's state.
func (p *StatePersister) loadProvider(provider StateProvider) error {
	filename := provider.Name() + ".json"
	path := filepath.Join(p.stateDir, filename)

	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read state file %s: %w", path, err)
	}

	var state stateFile
	if err := json.Unmarshal(raw, &state); err != nil {
		return fmt.Errorf("unmarshal state file %s: %w", path, err)
	}

	if state.Data == nil {
		return nil
	}

	// Re-marshal the data to get raw bytes, then unmarshal into proper type
	dataBytes, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("remarshal state data for %s: %w", provider.Name(), err)
	}

	// Create a new instance of the expected type by calling Save first
	// This is a workaround since we don't know the concrete type
	var genericData map[string]any
	if err := json.Unmarshal(dataBytes, &genericData); err != nil {
		return fmt.Errorf("unmarshal state data for %s: %w", provider.Name(), err)
	}

	return provider.Load(genericData)
}

// ResolveStateDir returns the state directory path.
// Priority:
// 1. STATE_PERSIST_DIR environment variable
// 2. /CLIProxyAPI/stats if running in Docker (container environment)
// 3. authDir/state if authDir provided
// 4. ~/.cli-proxy-api/state
func ResolveStateDir(authDir string) string {
	// Check environment variable first
	if dir := os.Getenv("STATE_PERSIST_DIR"); dir != "" {
		return dir
	}

	// Check if running in Docker container (by checking for /.dockerenv or /CLIProxyAPI directory)
	dockerDefault := "/CLIProxyAPI/stats"
	if _, err := os.Stat("/.dockerenv"); err == nil {
		// Running in Docker container
		_ = os.MkdirAll(dockerDefault, 0o755) // Ensure directory exists
		return dockerDefault
	}
	if _, err := os.Stat("/CLIProxyAPI"); err == nil {
		// /CLIProxyAPI exists, likely Docker environment
		_ = os.MkdirAll(dockerDefault, 0o755) // Ensure directory exists
		return dockerDefault
	}

	// Fall back to authDir/state or home directory
	if authDir != "" {
		return filepath.Join(authDir, stateDirName)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", stateDirName)
	}
	return filepath.Join(home, ".cli-proxy-api", stateDirName)
}

// Global persister instance
var (
	globalPersister     *StatePersister
	globalPersisterOnce sync.Once
	globalPersisterMu   sync.RWMutex
)

// InitGlobalPersister initializes the global persister.
func InitGlobalPersister(stateDir string, interval time.Duration) *StatePersister {
	globalPersisterOnce.Do(func() {
		globalPersister = NewStatePersister(stateDir, interval)
	})
	return globalPersister
}

// GetGlobalPersister returns the global persister instance.
func GetGlobalPersister() *StatePersister {
	globalPersisterMu.RLock()
	defer globalPersisterMu.RUnlock()
	return globalPersister
}

// RegisterGlobalProvider registers a provider with the global persister.
func RegisterGlobalProvider(provider StateProvider) {
	globalPersisterMu.RLock()
	defer globalPersisterMu.RUnlock()
	if globalPersister != nil {
		globalPersister.RegisterProvider(provider)
	}
}

// StartGlobalPersister starts the global persister.
func StartGlobalPersister() {
	globalPersisterMu.RLock()
	defer globalPersisterMu.RUnlock()
	if globalPersister != nil {
		globalPersister.Start()
	}
}

// StopGlobalPersister stops the global persister.
func StopGlobalPersister() {
	globalPersisterMu.RLock()
	defer globalPersisterMu.RUnlock()
	if globalPersister != nil {
		globalPersister.Stop()
	}
}

// LoadGlobalState loads all state from disk using the global persister.
func LoadGlobalState() error {
	globalPersisterMu.RLock()
	defer globalPersisterMu.RUnlock()
	if globalPersister != nil {
		return globalPersister.LoadAll()
	}
	return nil
}

// Context key for passing persister through context
type ctxKey struct{}

// WithPersister attaches a persister to the context.
func WithPersister(ctx context.Context, p *StatePersister) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext retrieves the persister from context.
func FromContext(ctx context.Context) *StatePersister {
	if p, ok := ctx.Value(ctxKey{}).(*StatePersister); ok {
		return p
	}
	return nil
}
