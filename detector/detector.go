package detector

import (
	"sync"
	"time"
)

// SuspicionLevel rappresenta il livello di sospetto (phi) per un peer.
type SuspicionLevel float64

// FailureDetector implementa un phi-accrual failure detector semplificato.
type FailureDetector struct {
	windowSize int
	mu         sync.RWMutex
	heartbeats map[string][]time.Time
}

// New crea un FailureDetector con una finestra di heartbeat e soglia non usata direttamente qui.
func New(windowSize int, threshold float64) *FailureDetector {
	return &FailureDetector{
		windowSize: windowSize,
		heartbeats: make(map[string][]time.Time),
	}
}

// HeartbeatNotify registra un heartbeat per peerID alla timestamp specificata.
func (fd *FailureDetector) HeartbeatNotify(peerID string, timestamp time.Time) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	times := fd.heartbeats[peerID]
	times = append(times, timestamp)
	if len(times) > fd.windowSize {
		times = times[len(times)-fd.windowSize:]
	}
	fd.heartbeats[peerID] = times
}

// Suspicion calcola un valore di phi proporzionale al gap tra il primo heartbeat nella finestra e il tempo corrente.
func (fd *FailureDetector) Suspicion(peerID string, now time.Time) SuspicionLevel {
	fd.mu.RLock()
	times, ok := fd.heartbeats[peerID]
	fd.mu.RUnlock()

	if !ok || len(times) < 2 {
		return 0.0
	}

	var total float64
	for i := 1; i < len(times); i++ {
		total += times[i].Sub(times[i-1]).Seconds()
	}
	mean := total / float64(len(times)-1)
	if mean == 0 {
		return 0.0
	}

	// Delta calcolato sul primo heartbeat nella finestra
	delta := now.Sub(times[0]).Seconds()
	phi := delta / mean
	return SuspicionLevel(phi)
}
