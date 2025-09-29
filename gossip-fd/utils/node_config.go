package utils

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	SelfID            string
	SelfAddr          string
	UDPPort           int // ← porta UDP derivata da SELF_ADDR
	GossipInterval    time.Duration
	HeartbeatInterval time.Duration
	SuspectTimeout    time.Duration
	DeadTimeout       time.Duration
	MaxDigestPeers    int

	// Service discovery
	APIPort          int
	ServicesCSV      string
	CalcPort         int
	ServiceTTL       int
	MaxServiceDigest int

	// Registry
	RegistryURL string

	// Death certificates a quorum
	QuorumK            int
	VoteWindow         time.Duration
	ObitTTL            time.Duration
	MaxVoteDigest      int
	MaxObitDigest      int
	ObitPriorityRounds int
	VotePriorityRounds int
}

func parseIntEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func parseDurationEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func mustEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func GetConfig() (Config, error) {
	id := mustEnv("SELF_ID", "")
	addr := mustEnv("SELF_ADDR", "") // "node1:9000"
	if id == "" || addr == "" {
		return Config{}, fmt.Errorf("SELF_ID e SELF_ADDR sono obbligatori")
	}
	_, portStr, ok := strings.Cut(addr, ":")
	if !ok || portStr == "" {
		return Config{}, fmt.Errorf("SELF_ADDR senza porta: %s", addr)
	}
	udpPort, err := strconv.Atoi(portStr)
	if err != nil {
		return Config{}, fmt.Errorf("porta invalida in SELF_ADDR: %v", err)
	}

	// default: API_PORT = porta UDP
	apiPort := parseIntEnv("API_PORT", udpPort)

	cfg := Config{
		SelfID:            id,
		SelfAddr:          addr,
		UDPPort:           udpPort,
		GossipInterval:    parseDurationEnv("GOSSIP_INTERVAL", 700*time.Millisecond),
		HeartbeatInterval: parseDurationEnv("HEARTBEAT_INTERVAL", 500*time.Millisecond),
		SuspectTimeout:    parseDurationEnv("SUSPECT_TIMEOUT", 2500*time.Millisecond),
		DeadTimeout:       parseDurationEnv("DEAD_TIMEOUT", 6000*time.Millisecond),
		MaxDigestPeers:    parseIntEnv("MAX_DIGEST", 64),

		APIPort:          apiPort,
		ServicesCSV:      mustEnv("SERVICES", ""),
		CalcPort:         parseIntEnv("CALC_PORT", 18080),
		ServiceTTL:       parseIntEnv("SERVICE_TTL", 15),
		MaxServiceDigest: parseIntEnv("MAX_SERVICE_DIGEST", 64),

		RegistryURL: mustEnv("REGISTRY_URL", "registry:8089"),

		QuorumK:            parseIntEnv("QUORUM_K", 2),
		VoteWindow:         parseDurationEnv("VOTE_WINDOW", 6*time.Second),
		ObitTTL:            parseDurationEnv("OBIT_TTL", 18*time.Second),
		MaxVoteDigest:      parseIntEnv("MAX_VOTE_DIGEST", 16),
		MaxObitDigest:      parseIntEnv("MAX_OBIT_DIGEST", 8),
		ObitPriorityRounds: parseIntEnv("OBIT_PRIORITY_ROUNDS", 3),
		VotePriorityRounds: parseIntEnv("VOTE_PRIORITY_ROUNDS", 2),
	}
	return cfg, nil
}
