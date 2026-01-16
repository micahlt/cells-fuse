package main

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/diamondburned/gotk4/pkg/gdk/v4"
	"github.com/diamondburned/gotk4/pkg/gio/v2"
	"github.com/diamondburned/gotk4/pkg/glib/v2"
	"github.com/diamondburned/gotk4/pkg/gtk/v4"
	"github.com/skratchdot/open-golang/open"
)

//go:embed assets/cells-color.png
var logoData []byte

func createMainWindow(app *gtk.Application, session *AppSession, mountSignal chan bool) *gtk.ApplicationWindow {
	window := gtk.NewApplicationWindow(app)
	window.SetTitle("Cells Fuse")
	window.SetDefaultSize(400, 300)

	box := gtk.NewBox(gtk.OrientationVertical, 10)
	box.SetMarginTop(20)
	box.SetMarginBottom(20)
	box.SetMarginStart(20)
	box.SetMarginEnd(20)
	window.SetChild(box)

	about := gtk.NewAboutDialog()
	about.SetAuthors([]string{"Micah Lindley"})
	about.SetComments("A FUSE filesystem for Pydio Cells")
	about.SetLicenseType(gtk.LicenseGPL30)
	about.SetWebsite("https://github.com/micahlt/cells-fuse")
	about.SetWebsiteLabel("GitHub repository")
	about.SetVersion("1.0.0")
	about.ConnectCloseRequest(func() bool {
		about.SetVisible(false)
		return true
	})

	bytes := glib.NewBytesWithGo(logoData)
	texture, err := gdk.NewTextureFromBytes(bytes)
	if err == nil {
		about.SetLogo(texture)
	}
	aboutButton := gtk.NewButtonFromIconName("settings")
	aboutButton.ConnectClicked(func() {
		about.SetVisible(true)
	})
	box.Append(aboutButton)

	urlLabel := gtk.NewLabel("Cells instance URL")
	urlLabel.SetHAlign(gtk.AlignStart)
	box.Append(urlLabel)

	entry := gtk.NewEntry()
	entry.SetPlaceholderText("https://your-cells-instance.com")
	if session.AppUrl != "" {
		entry.SetText(session.AppUrl)
	}
	box.Append(entry)

	mountLabel := gtk.NewLabel("Local mount point")
	mountLabel.SetHAlign(gtk.AlignStart)
	box.Append(mountLabel)

	mountBox := gtk.NewBox(gtk.OrientationHorizontal, 5)

	mountEntry := gtk.NewEntry()
	mountEntry.SetPlaceholderText("/home/user/cells")
	mountEntry.SetHExpand(true)
	if session.MountPoint != "" {
		mountEntry.SetText(session.MountPoint)
	}
	mountBox.Append(mountEntry)

	browseBtn := gtk.NewButtonWithLabel("Browse...")
	browseBtn.ConnectClicked(func() {
		dialog := gtk.NewFileDialog()

		dialog.SelectFolder(context.TODO(), &window.Window, func(res gio.AsyncResulter) {
			file, err := dialog.SelectFolderFinish(res)
			if err != nil {
				return
			}
			if file != nil {
				path := file.Path()
				mountEntry.SetText(path)
				session.MountPoint = path
			}
		})
	})
	mountBox.Append(browseBtn)
	box.Append(mountBox)

	statusLabel := gtk.NewLabel("Ready")
	box.Append(statusLabel)

	authBtn := gtk.NewButtonWithLabel("Authenticate")
	authBtn.ConnectClicked(func() {
		performAuth(entry.Text(), session, statusLabel)
	})
	box.Append(authBtn)

	mountBtn := gtk.NewButtonWithLabel("Start FUSE")
	if session.IsMounted {
		mountBtn.SetLabel("Stop FUSE")
	}
	box.Append(mountBtn)

	mountBtn.ConnectClicked(func() {
		session.MountPoint = mountEntry.Text()
		if err := SaveConfig(session); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
		}

		if session.IsMounted {
			mountSignal <- false
			mountBtn.SetLabel("Start FUSE")
			statusLabel.SetText("FUSE stopping...")
		} else {
			if session.AuthToken != "" {
				if session.MountPoint == "" {
					statusLabel.SetText("Please set a mount point")
					return
				}
				mountSignal <- true
				mountBtn.SetLabel("Stop FUSE")
				statusLabel.SetText("FUSE starting...")
			} else {
				statusLabel.SetText("Please authenticate first")
			}
		}
	})

	expander := gtk.NewExpander("Logs")
	box.Append(expander)

	scrolled := gtk.NewScrolledWindow()
	scrolled.SetMinContentHeight(150)
	scrolled.SetPolicy(gtk.PolicyAutomatic, gtk.PolicyAutomatic)
	expander.SetChild(scrolled)

	logView := gtk.NewTextView()
	logView.SetEditable(false)
	logView.SetWrapMode(gtk.WrapWord)
	logView.SetMonospace(true)
	scrolled.SetChild(logView)

	go func() {
		for msg := range session.LogChannel {
			// Capture msg for closure
			message := msg
			glib.IdleAdd(func() bool {
				buffer := logView.Buffer()
				end := buffer.EndIter()
				buffer.Insert(end, message+"\n")
				// Auto-scroll
				mark := buffer.CreateMark("end", end, false)
				logView.ScrollMarkOnscreen(mark)
				return false
			})
		}
	}()

	go func() {
		for errMsg := range session.MountErrorChannel {
			message := errMsg
			glib.IdleAdd(func() bool {
				mountBtn.SetLabel("Start FUSE")
				statusLabel.SetText("Mount failed")

				alertDialog := gtk.NewMessageDialog(window.Application().ActiveWindow(), gtk.DialogDestroyWithParent, gtk.MessageError, gtk.ButtonsClose)
				alertDialog.SetMarkup(message)
				alertDialog.SetModal(true)
				alertDialog.SetTitle("Error")
				alertDialog.ConnectResponse(func(responseID int) {
					alertDialog.Destroy()
				})
				alertDialog.SetVisible(true)
				return false
			})
		}
	}()

	window.ConnectCloseRequest(func() bool {
		window.SetVisible(false)
		// Return true to stop other handlers (like destruction)
		return true
	})

	return window
}

func performAuth(instanceURL string, session *AppSession, statusLabel *gtk.Label) {
	statusLabel.SetText("Opening browser for authentication...")

	resultChan, err := StartOAuthFlow(instanceURL, func(authURL string) {
		open.Run(authURL)
	})

	if err != nil {
		statusLabel.SetText(fmt.Sprintf("Error: %v", err))
		return
	}

	go func() {
		result := <-resultChan
		// Update UI on main thread
		glib.IdleAdd(func() bool {
			if result.Error != nil {
				statusLabel.SetText(fmt.Sprintf("Auth failed: %v", result.Error))
				return false
			}

			session.AuthToken = result.Token
			session.RefreshToken = result.RefreshToken
			session.TokenExpiry = int64(result.ExpiresAt)
			session.User = result.User
			session.AppUrl = instanceURL
			// session.MountPoint = "/home/micahlt/cells"

			statusLabel.SetText("Authentication successful!")

			// Save config
			if err := SaveConfig(session); err != nil {
				fmt.Printf("Error saving config: %v\n", err)
			}
			return false
		})
	}()
}
