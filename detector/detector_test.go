package detector

import (
	"testing"
	"time"
)

func TestRegularHeartbeatsKeepsSuspicionLow(t *testing.T) {
	// Configura il detector con windowSize=5, thresholdPhi=8.0
	fd := New(5, 8.0)
	peerID := "peer1"
	base := time.Now()
	interval := 1 * time.Second

	// Simula heartbeats regolari
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * interval)
		fd.HeartbeatNotify(peerID, ts)
	}

	// Controlla suspicion immediatamente dopo ultimo heartbeat
	now := base.Add(5 * interval)
	phi := fd.Suspicion(peerID, now)
	if phi >= 8.0 {
		t.Errorf("Expected phi < 8.0 for regular heartbeats, got %v", phi)
	}
}

func TestMissedHeartbeatIncreasesSuspicion(t *testing.T) {
	fd := New(5, 8.0)
	peerID := "peer2"
	base := time.Now()
	interval := 1 * time.Second

	// Simula heartbeats regolari
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * interval)
		fd.HeartbeatNotify(peerID, ts)
	}

	// Salta un intervallo multiplo
	now := base.Add(10 * interval)
	phi := fd.Suspicion(peerID, now)
	if phi <= 8.0 {
		t.Errorf("Expected phi > 8.0 after missing heartbeat, got %v", phi)
	}
}

func TestUnknownPeerReturnsZero(t *testing.T) {
	fd := New(5, 8.0)
	phi := fd.Suspicion("unknown", time.Now())
	if phi != 0.0 {
		t.Errorf("Expected phi=0.0 for unknown peer, got %v", phi)
	}
}
