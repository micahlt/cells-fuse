package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/diamondburned/gotk4/pkg/gio/v2"
	"github.com/diamondburned/gotk4/pkg/glib/v2"
	"github.com/diamondburned/gotk4/pkg/gtk/v4"
	"github.com/energye/systray"
	"github.com/pydio/cells-sdk-go/v5/apiv1/client"
	"github.com/winfsp/cgofuse/fuse"
)

type AppSession struct {
	PydioClient       *client.PydioCellsRestAPI
	AuthToken         string
	AppUrl            string
	S3Client          *s3.Client
	IsMounted         bool
	MountPoint        string
	FuseHost          *fuse.FileSystemHost
	RefreshToken      string
	TokenExpiry       int64
	User              string
	CellsFuse         *CellsFuse
	TrayUpdateSignal  chan bool
	LogChannel        chan string
	MountErrorChannel chan string
	// Performance tuning parameters
	ChunkSizeMB       int `json:"ChunkSizeMB"`       // ReadAheadSize in MB
	PrefetchChunks    int `json:"PrefetchChunks"`    // How many chunks to prefetch ahead
	CacheChunks       int `json:"CacheChunks"`       // How many chunks to keep in cache
	CacheTTLSeconds   int `json:"CacheTTLSeconds"`   // Metadata cache TTL in seconds
}

const AppID = "com.micahlindley.cells-fuse"
const AppVersion = "0.1.0"

func Log(session *AppSession, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(msg)
	select {
	case session.LogChannel <- msg:
	default:
		// Drop message if channel is full to prevent blocking
	}
}

func main() {
	fmt.Println("Hello world")

	session := &AppSession{
		MountPoint:        "~/cells",
		TrayUpdateSignal:  make(chan bool, 1),
		LogChannel:        make(chan string, 100),
		MountErrorChannel: make(chan string, 1),
		// Default performance settings
		ChunkSizeMB:    4,
		PrefetchChunks: 10,
		CacheChunks:    100,
		CacheTTLSeconds: 5,
	}

	if err := LoadConfig(session); err != nil {
		fmt.Printf("Warning: could not load config: %v\n", err)
	}

	mountSignal := make(chan bool, 1)
	go runFuseBackground(session, mountSignal)

	startTokenRefresher(session)

	app := gtk.NewApplication(AppID, gio.ApplicationFlagsNone)

	app.Hold()

	var mainWindow *gtk.ApplicationWindow
	var systrayStarted bool

	app.ConnectActivate(func() {
		if !systrayStarted {
			systrayStarted = true
			// Launch Systray in a separate goroutine.
			go func() {
				systray.Run(func() {
					setupTray(session, mountSignal, func() {
						// Show the window using glib idle to ensure thread safety
						glib.IdleAdd(func() bool {
							if mainWindow != nil {
								mainWindow.SetVisible(true)
								mainWindow.Present()
							}
							return false
						})
					})
				}, func() {
					if session.IsMounted && session.FuseHost != nil {
						fmt.Println("Unmounting FUSE before exit...")
						session.FuseHost.Unmount()
					}
					os.Exit(0)
				})
			}()
		}

		if mainWindow == nil {
			mainWindow = createMainWindow(app, session, mountSignal)
		}
		mainWindow.SetVisible(true)
	})

	if code := app.Run(os.Args); code > 0 {
		os.Exit(code)
	}
}
