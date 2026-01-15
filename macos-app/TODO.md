# Axiom FS macOS App - TODO

## ✅ NFS Mode (Implemented)

The app uses NFS mode which works without an Apple Developer account:
- Menu bar app spawns the `axiom-fs` Go binary as a subprocess
- Auto-mounts NFS share to `~/Axiom`
- Process lifecycle management (start/stop, PID tracking, crash handling)
- Binary discovery in `~/.local/bin`, `/usr/local/bin`, `/opt/homebrew/bin`, `~/go/bin`

To use:
1. Build the Go binary: `go build -o axiom-fs ./cmd/axiom-fs`
2. Install it: `cp axiom-fs ~/.local/bin/`
3. Run the menu bar app and click "Connect"

---

## 🔧 TODO

### Settings
- [ ] Token input → save to Keychain
- [ ] Load existing token on app launch
- [ ] Validate token by calling API on save

### Polish
- [ ] Connection status indicator with animation
- [ ] Show last sync time
- [ ] Show dataset count when connected
- [ ] Menu bar icon changes color based on status
- [ ] Notifications for errors

### Build & Distribution
- [ ] Create proper 1024x1024 app icon
- [ ] Create DMG for distribution
- [ ] README for macos-app/

### Testing
- [ ] Test with large datasets
- [ ] Test offline behavior
- [ ] Test concurrent Finder access
