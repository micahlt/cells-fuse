package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"

	"github.com/pydio/cells-sdk-go/v5/apiv1"
	"github.com/pydio/cells-sdk-go/v5/apiv1/transport/rest"
)

const (
	OAuthClientID    = "cells-sync"
	OAuthCallbackURL = "http://localhost:3000/servers/callback"
)

type OAuthResult struct {
	Token        string
	RefreshToken string
	ExpiresAt    int
	User         string
	Error        error
}

func generateState() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func StartOAuthFlow(instanceURL string, openBrowser func(authURL string)) (resultChan chan OAuthResult, err error) {
	// Validate URL
	if instanceURL == "" {
		return nil, fmt.Errorf("please enter an instance URL")
	}

	// Ensure URL has a scheme
	if _, err := url.Parse(instanceURL); err != nil {
		return nil, fmt.Errorf("invalid URL format")
	}

	state := generateState()

	// Prepare the OAuth URL
	authURL, err := rest.OAuthPrepareUrl(OAuthClientID, state, instanceURL, OAuthCallbackURL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare OAuth URL: %w", err)
	}

	// Create SDK config
	sdkConfig := &apiv1.SdkConfig{
		Url:        instanceURL,
		SkipVerify: false,
	}

	// Channel to receive the auth code
	codeChan := make(chan string, 1)
	errChan := make(chan error, 1)
	resultChan = make(chan OAuthResult, 1)

	// Start local HTTP server to handle callback
	mux := http.NewServeMux()
	server := &http.Server{Addr: ":3000", Handler: mux}

	mux.HandleFunc("/servers/callback", func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		returnedState := r.URL.Query().Get("state")

		if returnedState != state {
			errChan <- fmt.Errorf("state mismatch")
			w.Write([]byte("Authentication failed: state mismatch"))
			return
		}

		if code == "" {
			errChan <- fmt.Errorf("no code received")
			w.Write([]byte("Authentication failed: no code received"))
			return
		}

		codeChan <- code
		w.Write([]byte("Authentication successful! You can close this window."))
	})

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Open browser via the provided callback
	openBrowser(authURL)

	// Wait for callback in a goroutine
	go func() {
		select {
		case code := <-codeChan:
			// Exchange code for tokens
			err := rest.OAuthExchangeCode(sdkConfig, OAuthClientID, code, OAuthCallbackURL)
			server.Shutdown(context.Background())

			if err != nil {
				resultChan <- OAuthResult{Error: fmt.Errorf("token exchange failed: %w", err)}
				return
			}

			fmt.Printf("DEBUG OAuth: Token received, length=%d\n", len(sdkConfig.IdToken))
			fmt.Printf("DEBUG OAuth: Token prefix: %s...\n", sdkConfig.IdToken[:min(50, len(sdkConfig.IdToken))])
			fmt.Printf("DEBUG OAuth: RefreshToken length=%d\n", len(sdkConfig.RefreshToken))
			fmt.Printf("DEBUG OAuth: User=%s\n", sdkConfig.User)

			resultChan <- OAuthResult{
				Token:        sdkConfig.IdToken,
				RefreshToken: sdkConfig.RefreshToken,
				ExpiresAt:    sdkConfig.TokenExpiresAt,
				User:         sdkConfig.User,
			}
			fmt.Printf("Authenticated! Token expires at: %d\n", sdkConfig.TokenExpiresAt)

		case err := <-errChan:
			server.Shutdown(context.Background())
			resultChan <- OAuthResult{Error: err}
		}
	}()

	return resultChan, nil
}
