package main

import (
	"os"
	"strconv"
	"strings"
)

type RegistryConfig struct {
	port int
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

func GetRegistryConfig() RegistryConfig {
	cfg := RegistryConfig{
		port: parseIntEnv("PORT", 8089),
	}
	return cfg
}
