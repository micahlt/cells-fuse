package main

import (
	_ "embed"

	"github.com/energye/systray"
	"github.com/skratchdot/open-golang/open"
)

//go:embed assets/cells-white.png
var iconData []byte

func setupTray(session *AppSession, mountSignal chan bool, showWindow func()) {
	// Configure tray
	systray.SetIcon(iconData)
	systray.SetTitle("Cells")
	systray.SetTooltip("Pydio Cells Fuse")

	// Menu Items
	mConnect := systray.AddMenuItem("Connect", "Connect to Pydio Cells")
	mConfigure := systray.AddMenuItem("Configure", "Open Configuration Window")
	systray.AddSeparator()
	mOpen := systray.AddMenuItem("Open Mount Folder", "Open the local directory")
	mQuit := systray.AddMenuItem("Quit", "Quit the application")

	// Event Listeners (Callbacks for energye/systray)
	mConnect.Click(func() {
		if session.IsMounted {
			mountSignal <- false
		} else {
			if session.AuthToken != "" && session.MountPoint != "" {
				mountSignal <- true
			} else {
				showWindow()
			}
		}
	})

	mConfigure.Click(func() {
		showWindow()
	})

	mOpen.Click(func() {
		if session.MountPoint != "" {
			open.Run(expandPath(session.MountPoint))
		}
	})

	mQuit.Click(func() {
		systray.Quit()
		// os.Exit(0) // handled in main
	})

	// Logic for dynamic updates (Keep the loop for title updates)
	go func() {
		for {
			if session.IsMounted {
				mConnect.SetTitle("Disconnect")
			} else {
				mConnect.SetTitle("Connect")
			}
			<-session.TrayUpdateSignal
		}
	}()

	// Initial update
	if session.TrayUpdateSignal != nil {
		// Non-blocking send/check
		select {
		case session.TrayUpdateSignal <- true:
		default:
		}
	}
}
