package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type NodeConfig struct {
	SelfID            string
	SelfAddr          string
	UDPPort           int
	GossipInterval    time.Duration
	HeartbeatInterval time.Duration
	SuspectTimeout    time.Duration
	DeadTimeout       time.Duration
	MaxDigestPeers    int
	FanoutK           int

	// Service discovery
	APIPort               int
	Services              string
	CalcPort              int
	ServiceTTL            time.Duration
	ServiceRefreshTimeout time.Duration
	MaxServiceDigest      int

	// Registry
	RegistryURL string

	// Death certificates a quorum
	QuorumK            int
	VoteWindow         time.Duration
	CertTTL            time.Duration
	MaxVoteDigest      int
	MaxCertDigest      int
	CertPriorityRounds int
	VotePriorityRounds int
}

func GetNodeConfig() (NodeConfig, error) {
	id := mustEnv("SELF_ID", "")
	addr := mustEnv("SELF_ADDR", "") // "node1:9000"
	if id == "" || addr == "" {
		return NodeConfig{}, fmt.Errorf("SELF_ID e SELF_ADDR are mandatory")
	}
	_, portStr, ok := strings.Cut(addr, ":")
	if !ok || portStr == "" {
		return NodeConfig{}, fmt.Errorf("SELF_ADDR without port: %s", addr)
	}
	udpPort, err := strconv.Atoi(portStr)
	if err != nil {
		return NodeConfig{}, fmt.Errorf("invalid port in SELF_ADDR: %v", err)
	}

	apiPort := parseIntEnv("API_PORT", udpPort)

	cfg := NodeConfig{
		SelfID:            id,
		SelfAddr:          addr,
		UDPPort:           udpPort,
		GossipInterval:    parseDurationEnv("GOSSIP_INTERVAL", 700*time.Millisecond),
		HeartbeatInterval: parseDurationEnv("HEARTBEAT_INTERVAL", 500*time.Millisecond),
		SuspectTimeout:    parseDurationEnv("SUSPECT_TIMEOUT", 2500*time.Millisecond),
		DeadTimeout:       parseDurationEnv("DEAD_TIMEOUT", 6000*time.Millisecond),
		MaxDigestPeers:    parseIntEnv("MAX_DIGEST_PEERS", 64),
		FanoutK:           parseIntEnv("FANOUT_K", 0),

		APIPort:               apiPort,
		Services:              mustEnv("SERVICES", ""),
		CalcPort:              parseIntEnv("CALC_PORT", 18080),
		ServiceTTL:            parseDurationEnv("SERVICE_TTL", 15),
		ServiceRefreshTimeout: parseDurationEnv("SERVICE_REFRESH_TIMEOUT", 5*time.Second),
		MaxServiceDigest:      parseIntEnv("MAX_SERVICE_DIGEST", 64),

		RegistryURL: mustEnv("REGISTRY_URL", "registry:8089"),

		QuorumK:            parseIntEnv("QUORUM_K", 2),
		VoteWindow:         parseDurationEnv("VOTE_WINDOW", 6*time.Second),
		CertTTL:            parseDurationEnv("CERT_TTL", 18*time.Second),
		MaxVoteDigest:      parseIntEnv("MAX_VOTE_DIGEST", 16),
		MaxCertDigest:      parseIntEnv("MAX_CERT_DIGEST", 8),
		CertPriorityRounds: parseIntEnv("CERT_PRIORITY_ROUNDS", 3),
		VotePriorityRounds: parseIntEnv("VOTE_PRIORITY_ROUNDS", 2),
	}
	return cfg, nil
}
