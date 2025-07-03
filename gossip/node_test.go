package gossip

import (
	"testing"
	"time"
)

func TestTwoNodesConverge(t *testing.T) {
	// Configuro due nodi su porte diverse
	n1, err := Config("A", "127.0.0.1:8001", 100*time.Millisecond, []string{"127.0.0.1:8002"})
	if err != nil {
		t.Fatalf("Config n1 failed: %v", err)
	}
	n2, err := Config("B", "127.0.0.1:8002", 100*time.Millisecond, []string{"127.0.0.1:8001"})
	if err != nil {
		t.Fatalf("Config n2 failed: %v", err)
	}

	// Avvio entrambi i nodi
	if err := n1.Start(); err != nil {
		t.Fatalf("Start n1 failed: %v", err)
	}
	if err := n2.Start(); err != nil {
		t.Fatalf("Start n2 failed: %v", err)
	}

	// Attendo un paio di tick perché si scambino gossip
	time.Sleep(500 * time.Millisecond)

	// Controllo che entrambi conoscano l'altro
	n1.mu.RLock()
	defer n1.mu.RUnlock()
	n2.mu.RLock()
	defer n2.mu.RUnlock()

	if len(n1.members) != 2 {
		t.Errorf("n1.members len = %d, want 2", len(n1.members))
	}
	if len(n2.members) != 2 {
		t.Errorf("n2.members len = %d, want 2", len(n2.members))
	}
	if _, ok := n1.members["B"]; !ok {
		t.Error("n1 is missing member B")
	}
	if _, ok := n2.members["A"]; !ok {
		t.Error("n2 is missing member A")
	}
}
