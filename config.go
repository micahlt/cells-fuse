package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func getConfigPath() (string, error) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	appDir := filepath.Join(configDir, "cells-fuse")
	if err := os.MkdirAll(appDir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(appDir, "config.json"), nil
}

type AppConfig struct {
	AuthToken       string          `json:"AuthToken"`
	RefreshToken    string          `json:"RefreshToken"`
	TokenExpiry     int64           `json:"TokenExpiry"`
	MountPoint      string          `json:"MountPoint"`
	AppUrl          string          `json:"AppUrl"`
	User            string          `json:"User"`
	ChunkSizeMB     int             `json:"ChunkSizeMB,omitempty"`
	PrefetchChunks  int             `json:"PrefetchChunks,omitempty"`
	CacheChunks     int             `json:"CacheChunks,omitempty"`
	CacheTTLSeconds int             `json:"CacheTTLSeconds,omitempty"`
	LogConfig       map[string]bool `json:"LogConfig"`
}

func LoadConfig(session *AppSession) error {
	path, err := getConfigPath()
	if err != nil {
		return err
	}

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var config AppConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}

	session.AuthToken = config.AuthToken
	session.RefreshToken = config.RefreshToken
	session.TokenExpiry = config.TokenExpiry
	session.MountPoint = config.MountPoint
	session.AppUrl = config.AppUrl
	session.User = config.User

	// Merge log config
	if config.LogConfig != nil {
		for k, v := range config.LogConfig {
			session.LogConfig[k] = v
		}
	}

	// Load performance settings with defaults if not set
	if config.ChunkSizeMB > 0 {
		session.ChunkSizeMB = config.ChunkSizeMB
	}
	if config.PrefetchChunks > 0 {
		session.PrefetchChunks = config.PrefetchChunks
	}
	if config.CacheChunks > 0 {
		session.CacheChunks = config.CacheChunks
	}
	if config.CacheTTLSeconds > 0 {
		session.CacheTTLSeconds = config.CacheTTLSeconds
	}

	return nil
}

func SaveConfig(session *AppSession) error {
	path, err := getConfigPath()
	if err != nil {
		return err
	}

	config := AppConfig{
		AuthToken:       session.AuthToken,
		RefreshToken:    session.RefreshToken,
		TokenExpiry:     session.TokenExpiry,
		MountPoint:      session.MountPoint,
		AppUrl:          session.AppUrl,
		User:            session.User,
		ChunkSizeMB:     session.ChunkSizeMB,
		PrefetchChunks:  session.PrefetchChunks,
		CacheChunks:     session.CacheChunks,
		CacheTTLSeconds: session.CacheTTLSeconds,
		LogConfig:       session.LogConfig,
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
