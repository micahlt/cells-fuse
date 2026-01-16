package main

import (
	"time"

	"github.com/pydio/cells-sdk-go/v5/apiv1"
	"github.com/pydio/cells-sdk-go/v5/apiv1/transport/rest"
)

func startTokenRefresher(session *AppSession) {
	go checkAndRefreshToken(session)

	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			checkAndRefreshToken(session)
		}
	}()
}

func checkAndRefreshToken(session *AppSession) {
	if session.AuthToken == "" || session.RefreshToken == "" {
		return
	}

	expiry := time.Unix(session.TokenExpiry, 0)
	if time.Until(expiry) > 15*time.Minute {
		return
	}

	Log(session, "Token is expiring at %v, refreshing now...", expiry)

	conf := &apiv1.SdkConfig{
		Url:          session.AppUrl,
		IdToken:      session.AuthToken,
		RefreshToken: session.RefreshToken,
		SkipVerify:   false,
	}

	success, err := rest.RefreshJwtToken("cells-sync", conf)
	if err != nil {
		Log(session, "Error refreshing token: %v", err)
		return
	}

	Log(session, "Token Refresh Success: %v", success)

	session.AuthToken = conf.IdToken
	session.RefreshToken = conf.RefreshToken
	session.TokenExpiry = int64(conf.TokenExpiresAt)

	Log(session, "Token refreshed! New expiry: %v", time.Unix(session.TokenExpiry, 0))

	if err := SaveConfig(session); err != nil {
		Log(session, "Error saving config after refresh: %v", err)
	}

	if session.CellsFuse != nil {
		Log(session, "Updating FUSE clients with new token...")
		session.CellsFuse.UpdateClients(session)
	}
}
