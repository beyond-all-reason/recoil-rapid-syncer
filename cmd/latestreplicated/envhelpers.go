package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

func getDurationEnv(name string, defaultValue time.Duration) time.Duration {
	envDurationStr := os.Getenv(name)
	if envDurationStr == "" {
		return defaultValue
	}
	duration, err := time.ParseDuration(envDurationStr)
	if err != nil {
		log.Fatalf("Failed to parse %s: %v", name, err)
	}
	return duration
}

func getFloatEnv(name string, defaultValue float64) float64 {
	envFloatStr := os.Getenv(name)
	if envFloatStr == "" {
		return defaultValue
	}
	envFloat, err := strconv.ParseFloat(envFloatStr, 64)
	if err != nil {
		log.Fatalf("Failed to parse %s: %v", name, err)
	}
	return envFloat
}

func getStrEnv(name string, defaultValue string) string {
	envStr := os.Getenv(name)
	if envStr == "" {
		return defaultValue
	}
	return envStr
}

func getRequiredStrEnv(name string) string {
	envStr := os.Getenv(name)
	if envStr == "" {
		log.Fatalf("Missing required env variable %s", name)
	}
	return envStr
}
