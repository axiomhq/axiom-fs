# Axiom FS macOS App - TODO

## 🚨 Critical: Apple Developer Account & Code Signing

The File Provider extension **will not work** without proper code signing, which requires an Apple Developer account.

### Getting an Apple Developer Account

**Option 1: Free Apple ID (Limited)**
- Sign in to Xcode with any Apple ID
- Can build and run on your own Mac
- ⚠️ File Provider extensions may NOT work (Apple restricts some capabilities)
- No distribution outside your machine

**Option 2: Apple Developer Program ($99/year)**
- https://developer.apple.com/programs/enroll/
- Required for: File Provider, App Groups, Keychain Sharing, Notarization
- Process:
  1. Go to https://developer.apple.com/account
  2. Sign in with Apple ID
  3. Enroll in Apple Developer Program
  4. Pay $99 USD/year (individuals) or $299/year (organizations)
  5. Wait 24-48 hours for approval
  6. Your Apple ID becomes your "Team"

### After You Have a Developer Account

1. Open `macos-app/AxiomFS/AxiomFS.xcodeproj` in Xcode
2. Xcode → Settings → Accounts → Add your Apple ID
3. Select the **AxiomFS** target → Signing & Capabilities
   - Set Team to your name/organization
   - Enable "Automatically manage signing"
   - Add capability: **App Groups** → `group.com.axiom.fs`
   - Add capability: **Keychain Sharing**
   - Add capability: **Outgoing Connections (Client)**
4. Select the **AxiomProvider** target → Signing & Capabilities
   - Set same Team
   - Add capability: **App Groups** → `group.com.axiom.fs`
   - Add capability: **Keychain Sharing**
   - Add capability: **File Provider**
   - Add capability: **Outgoing Connections (Client)**
5. Build and run

### Alternative: NFS Mode (Implemented)

The app now includes NFS fallback mode that works without a developer account:
- Menu bar app spawns the `axiom-fs` Go binary as a subprocess
- Auto-mounts NFS share to `/Volumes/Axiom`
- Process lifecycle management (start/stop, PID tracking, crash handling)
- Binary discovery in `/usr/local/bin`, `/opt/homebrew/bin`, `~/go/bin`, or bundled

To use NFS mode:
1. Build the Go binary: `go build -o axiom-fs ./cmd/axiom-fs`
2. Install it: `mv axiom-fs /usr/local/bin/` (or add to PATH)
3. Run the menu bar app and click "Connect"

---

## 🔧 Cut Corners to Address

### AxiomKit Not Integrated
- [ ] Add AxiomKit as SPM dependency to both Xcode targets
- [ ] Wire up `AxiomClient` in `FileProviderExtension`
- [ ] Wire up `AxiomConfig.load()` to read `~/.axiom.toml`
- [ ] Use `KeychainManager` for token storage in Settings

### File Provider Extension (Skeleton Only)
- [ ] **Enumerate real datasets** - call `AxiomClient.listDatasets()`
- [ ] **Fetch real content** - execute APL queries in `fetchContents()`
- [ ] **Stable identifiers** - persist item→identifier mapping (SQLite or plist)
- [ ] **Metadata caching** - avoid API calls on every enumeration
- [ ] **Error mapping** - translate `AxiomError` to `NSFileProviderError`

### Write Support for `_queries`
- [ ] Implement `createItem` → save APL file locally + Axiom saved queries API
- [ ] Implement `modifyItem` → update query
- [ ] Implement `deleteItem` → remove query
- [ ] Decide: are queries stored locally or synced to Axiom?

### Settings Not Functional
- [ ] Token input → save to Keychain via `KeychainManager`
- [ ] Load existing token on app launch
- [ ] Validate token by calling `listDatasets()` on save
- [ ] Show validation result (success/error)

### Credential Sharing (App ↔ Extension)
- [ ] Both targets must have matching Keychain access group
- [ ] Test that extension can read token saved by main app
- [ ] Handle token expiry/revocation gracefully

---

## 📦 Build & Distribution

### App Icon
- [ ] Create proper 1024x1024 app icon (icns)
- [ ] Add AppIcon to Assets.xcassets
- [ ] Menu bar icon works but could use refinement

### Notarization
- [ ] Set up Developer ID certificate
- [ ] Enable Hardened Runtime (after signing works)
- [ ] Run `notarytool submit`
- [ ] Staple notarization ticket

### DMG Creation
- [ ] Create DMG background image
- [ ] Use `create-dmg` for pretty installer
- [ ] Test on clean macOS install

---

## 🧪 Testing

### Unit Tests
- [ ] AxiomClient API tests (mock URLSession)
- [ ] Configuration parsing tests
- [ ] Keychain tests

### Integration Tests
- [ ] File Provider enumeration with real API
- [ ] Query execution and file materialization
- [ ] Write operations

### Manual Testing
- [ ] Test with large datasets
- [ ] Test offline behavior
- [ ] Test token expiry mid-session
- [ ] Test concurrent Finder access

---

## 🎨 Polish

### UI
- [ ] Connection status indicator with animation
- [ ] Show last sync time
- [ ] Show dataset count when connected
- [ ] Error details in expandable section

### UX
- [ ] First-run onboarding (enter token)
- [ ] Menu bar icon changes color based on status
- [ ] Notifications for errors

### Performance
- [ ] Request deduplication in AxiomClient
- [ ] Aggressive caching with TTL
- [ ] Background refresh of dataset list

---

## 📝 Documentation

- [ ] README for macos-app/
- [ ] Screenshots for app store / GitHub
- [ ] User guide: how to get API token
- [ ] Developer guide: building from source
