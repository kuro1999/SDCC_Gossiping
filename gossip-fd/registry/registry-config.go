package main

type RegistryConfig struct {
	port int
}

func GetRegistryConfig() RegistryConfig {
	cfg := RegistryConfig{
		port: parseIntEnv("PORT", 8089),
	}
	return cfg
}
