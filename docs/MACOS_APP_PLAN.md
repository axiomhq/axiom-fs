# Axiom FS macOS App Plan

> **Last Updated:** Vetted with Oracle and Librarian analysis of Apple File Provider documentation,
> Axiom API docs, and implementation patterns.

## Executive Summary

Transform axiom-fs from a CLI-driven NFS server into a native macOS app with Dropbox-like UX:
- **Menu bar app** showing connection status
- **File Provider extension** for native Finder integration
- **Automatic mounting** at login
- **Code-signed and notarized** for distribution

---

## Architecture Overview

```
Axiom FS.app/
├── Contents/
│   ├── MacOS/
│   │   └── Axiom FS              # SwiftUI menu bar app
│   ├── PlugIns/
│   │   └── AxiomProvider.appex   # File Provider extension
│   ├── Resources/
│   │   ├── axiom-icon.icns
│   │   └── Assets.xcassets
│   ├── Info.plist
│   └── Entitlements/
│       ├── App.entitlements
│       └── Provider.entitlements
```

**No Go, no XPC, no CGO.** Pure Swift calling Axiom HTTP API directly.

---

## Component Details

### 1. Menu Bar App (SwiftUI)

**Purpose**: System tray presence, status display, preferences

**Technology**: SwiftUI `MenuBarExtra` (macOS 13+)

**Features**:
| Feature | Implementation |
|---------|----------------|
| Menu bar icon | SF Symbol or custom icon showing connection state |
| Connection status | Green/yellow/red indicator |
| Quick actions | Connect/Disconnect, Open in Finder |
| Preferences | Credentials, default time range, cache settings |
| Quit | Clean shutdown of backend |

**Key Code Pattern**:
```swift
@main
struct AxiomFSApp: App {
    @StateObject private var connectionState = ConnectionState()
    
    var body: some Scene {
        MenuBarExtra {
            ContentView()
                .environmentObject(connectionState)
        } label: {
            Image(systemName: connectionState.isConnected ? "externaldrive.fill.badge.checkmark" : "externaldrive.badge.xmark")
        }
        .menuBarExtraStyle(.window)
        
        Settings {
            SettingsView()
        }
    }
}
```

**Info.plist Settings**:
```xml
<key>LSUIElement</key>
<true/>  <!-- Hide from Dock -->
<key>LSBackgroundOnly</key>
<false/>
```

---

### 2. File Provider Extension

**Purpose**: Native Finder integration without FUSE/NFS

**API**: `NSFileProviderReplicatedExtension` (macOS 11+, targeting macOS 13+)

**Reference**: Apple's [FruitBasket sample code](https://developer.apple.com/documentation/fileprovider/synchronizing-files-using-file-provider-extensions)

#### Why NSFileProviderReplicatedExtension?

| Option | Availability | Best For | Our Use Case |
|--------|-------------|----------|--------------|
| `NSFileProviderExtension` (Legacy) | macOS 10.15+ | Streaming, stateless | ❌ Old, avoid |
| `NSFileProviderReplicatedExtension` | macOS 11+ | Full replica + write support | ✅ **Required for `_queries`** |
| `NSFileProviderNonReplicatedExtension` | macOS 13+ | Read-only virtual FS | ❌ No write support |

**Decision:** Target **macOS 13+** but use `NSFileProviderReplicatedExtension` because `_queries` must be writable.

#### Implementation Notes

> **"Replicated" means local files, not virtual streaming.**
> - System expects items to be **materialized into actual local files** when accessed
> - You must write bytes to disk and return a file URL
> - Finder will read files more often than you expect (QuickLook, previews, icon generation)
> - Design to avoid triggering expensive queries from normal Finder behavior

**Stable Item Identifiers Required:**
- `NSFileProviderItemIdentifier` must be stable across app launches
- Use dataset UUIDs from Axiom, not array indices
- Requires a persistent metadata store (SQLite recommended) in the App Group container

**Finder Access Patterns to Handle:**
- Directory enumeration (cache metadata, don't run queries)
- File content fetch on-demand (this is where queries execute)
- QuickLook previews
- Multiple apps reading simultaneously

**Write Support for `_queries`:**
- Implement `createItem`, `modifyItem`, `deleteItem`
- Handle upload to Axiom (saved queries API)
- Conflict resolution if query modified elsewhere

#### Required Protocols

| Protocol | Purpose |
|----------|---------|
| `NSFileProviderReplicatedExtension` | Core extension lifecycle |
| `NSFileProviderEnumerating` | List directory contents |
| `NSFileProviderItemProtocol` | Item metadata (name, size, type) |
| `NSFileProviderEnumerator` | Enumerate items from backend |

#### Required Methods

```swift
// Extension lifecycle
init(domain: NSFileProviderDomain)
func invalidate()

// Item access
func item(for identifier: NSFileProviderItemIdentifier, 
          request: NSFileProviderRequest,
          completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress

// Fetch file contents on-demand
func fetchContents(for itemIdentifier: NSFileProviderItemIdentifier,
                   version requestedVersion: NSFileProviderItemVersion?,
                   request: NSFileProviderRequest,
                   completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress

// Directory enumeration
func enumerator(for containerItemIdentifier: NSFileProviderItemIdentifier,
                request: NSFileProviderRequest) throws -> NSFileProviderEnumerator

// Write support for _queries (required for ReplicatedExtension)
func createItem(basedOn itemTemplate: NSFileProviderItem,
                fields: NSFileProviderItemFields,
                contents url: URL?,
                options: NSFileProviderCreateItemOptions,
                request: NSFileProviderRequest,
                completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress

func modifyItem(_ item: NSFileProviderItem,
                baseVersion version: NSFileProviderItemVersion,
                changedFields: NSFileProviderItemFields,
                contents newContents: URL?,
                options: NSFileProviderModifyItemOptions,
                request: NSFileProviderRequest,
                completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress

func deleteItem(identifier: NSFileProviderItemIdentifier,
                baseVersion version: NSFileProviderItemVersion,
                options: NSFileProviderDeleteItemOptions,
                request: NSFileProviderRequest,
                completionHandler: @escaping (Error?) -> Void) -> Progress
```

#### Item Mapping

| Axiom FS Path | File Provider Item |
|---------------|-------------------|
| `/datasets` | Root container |
| `/datasets/{name}` | Folder |
| `/datasets/{name}/schema.csv` | File (dynamic content) |
| `/datasets/{name}/q/...` | Virtual query folder tree |
| `/_queries/{name}` | Writable folder |

#### Domain Management

```swift
let domain = NSFileProviderDomain(
    identifier: NSFileProviderDomainIdentifier("com.axiom.fs"),
    displayName: "Axiom"
)

// Add domain (shows in Finder sidebar)
try await NSFileProviderManager.add(domain)

// Remove domain
try await NSFileProviderManager.remove(domain)
```

#### Finder Sidebar Icon

In `Info.plist` of the extension:
```xml
<key>CFBundleIcons</key>
<dict>
    <key>CFBundlePrimaryIcon</key>
    <dict>
        <key>CFBundleSymbolName</key>
        <string>chart.bar.doc.horizontal</string>
    </dict>
</dict>
```

---

### 3. Axiom API Client (Pure Swift)

**No Go required.** The Axiom API is standard HTTP/REST - call it directly from Swift.

#### API Verification (from official docs)

| Endpoint | Method | Base URL | Notes |
|----------|--------|----------|-------|
| List datasets | GET | `https://api.axiom.co/v1/datasets` | Standard base |
| Query (APL) | POST | `https://api.axiom.co/v1/datasets/_apl` | Use `?format=tabular` or `?format=legacy` |
| Ingest | POST | `https://{region}.aws.edge.axiom.co/v1/datasets/{name}/ingest` | Edge deployment base |

**Authentication:**
- API Token: `Authorization: Bearer {token}`
- PAT: `Authorization: Bearer {pat}` + `X-Axiom-Org-Id: {orgId}`

#### Swift API Client

```swift
import Foundation

class AxiomClient {
    let baseURL: URL
    let token: String
    let orgID: String?
    
    init(url: URL = URL(string: "https://api.axiom.co")!, token: String, orgID: String? = nil) {
        self.baseURL = url
        self.token = token
        self.orgID = orgID
    }
    
    // List datasets
    func listDatasets() async throws -> [Dataset] {
        let url = baseURL.appendingPathComponent("v1/datasets")
        var request = URLRequest(url: url)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        if let orgID { request.setValue(orgID, forHTTPHeaderField: "X-Axiom-Org-Id") }
        
        let (data, response) = try await URLSession.shared.data(for: request)
        try validateResponse(response)
        return try JSONDecoder().decode([Dataset].self, from: data)
    }
    
    // Execute APL query
    // Docs: POST to /v1/datasets/_apl with format query param
    func query(apl: String, format: QueryFormat = .tabular) async throws -> Data {
        var components = URLComponents(url: baseURL.appendingPathComponent("v1/datasets/_apl"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "format", value: format.rawValue)]
        
        var request = URLRequest(url: components.url!)
        request.httpMethod = "POST"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let orgID { request.setValue(orgID, forHTTPHeaderField: "X-Axiom-Org-Id") }
        
        let body = ["apl": apl]
        request.httpBody = try JSONEncoder().encode(body)
        
        let (data, response) = try await URLSession.shared.data(for: request)
        try validateResponse(response)
        return data
    }
    
    private func validateResponse(_ response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else { return }
        switch http.statusCode {
        case 200..<300: return
        case 403: throw AxiomError.authenticationFailed
        case 429: throw AxiomError.rateLimited
        default: throw AxiomError.httpError(http.statusCode)
        }
    }
}

enum QueryFormat: String {
    case tabular = "tabular"
    case legacy = "legacy"
}

enum AxiomError: Error {
    case authenticationFailed
    case rateLimited
    case httpError(Int)
}
```

#### Models

```swift
struct Dataset: Codable {
    let name: String
    let description: String?
    let created: Date?
}

struct QueryResult: Codable {
    let status: QueryStatus
    let matches: [[String: AnyCodable]]?
}

struct QueryStatus: Codable {
    let elapsedTime: Int
    let rowsExamined: Int
    let rowsMatched: Int
}
```

#### Credential Loading from ~/.axiom.toml

```swift
import Foundation

struct AxiomConfig {
    let url: String?
    let token: String
    let orgID: String?
    
    static func load() throws -> AxiomConfig? {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let configPath = home.appendingPathComponent(".axiom.toml")
        
        guard FileManager.default.fileExists(atPath: configPath.path) else {
            return nil
        }
        
        let content = try String(contentsOf: configPath)
        // Parse TOML (use a Swift TOML library like TOMLKit)
        // Extract active_deployment and its settings
        return try parseTOML(content)
    }
}
```

#### Why Pure Swift?

| Aspect | CGO/Go | Pure Swift |
|--------|--------|------------|
| Complexity | High (bridging, XPC, build system) | Low |
| Binary size | Large (Go runtime) | Small |
| App Store | Problematic | Compatible |
| Debugging | Hard | Easy |
| Maintenance | Two languages | One language |
| Build time | Slow | Fast |

---

### 4. App Group & Shared Container

**Purpose**: Share data between app and extension

**Setup**:
1. Create App Group in Apple Developer Portal: `group.com.axiom.fs`
2. Add to entitlements for both app and extension:

```xml
<key>com.apple.security.application-groups</key>
<array>
    <string>group.com.axiom.fs</string>
</array>
```

**Shared Data**:
- Credentials (Keychain with access group)
- Configuration (UserDefaults with suite name)
- Cache directory

```swift
let sharedDefaults = UserDefaults(suiteName: "group.com.axiom.fs")
let sharedContainer = FileManager.default
    .containerURL(forSecurityApplicationGroupIdentifier: "group.com.axiom.fs")
```

---

### 5. Launch at Login

**Modern Approach** (macOS 13+): `SMAppService`

```swift
import ServiceManagement

// Enable
try SMAppService.mainApp.register()

// Disable
try SMAppService.mainApp.unregister()

// Check status
let status = SMAppService.mainApp.status
// .enabled, .notRegistered, .requiresApproval
```

**Legacy Approach** (macOS 10.x-12): Login Item helper bundle with `SMLoginItemSetEnabled`

---

### 6. Keychain Credentials Storage

> **Important:** App Groups (`group.com.axiom.fs`) and Keychain access groups are **different**.
> For sharing Keychain items between app and extension, you must:
> 1. Enable **Keychain Sharing** capability on both targets in Xcode
> 2. Use the Keychain access group (prefixed with Team ID), not the App Group ID

```swift
import Security

// Keychain access group format: $(AppIdentifierPrefix)com.axiom.fs
// The AppIdentifierPrefix is your Team ID followed by a dot, e.g., "ABCD1234.com.axiom.fs"
let keychainAccessGroup = "\(Bundle.main.infoDictionary?["AppIdentifierPrefix"] as? String ?? "")com.axiom.fs"

func saveToken(_ token: String, for url: String) throws {
    let query: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: "com.axiom.fs",
        kSecAttrAccount as String: url,
        kSecValueData as String: token.data(using: .utf8)!,
        kSecAttrAccessGroup as String: keychainAccessGroup  // NOT "group.com.axiom.fs"
    ]
    
    // Delete existing item first
    SecItemDelete(query as CFDictionary)
    
    let status = SecItemAdd(query as CFDictionary, nil)
    guard status == errSecSuccess else {
        throw NSError(domain: NSOSStatusErrorDomain, code: Int(status))
    }
}

func loadToken(for url: String) throws -> String? {
    let query: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: "com.axiom.fs",
        kSecAttrAccount as String: url,
        kSecAttrAccessGroup as String: keychainAccessGroup,
        kSecReturnData as String: true
    ]
    var result: AnyObject?
    let status = SecItemCopyMatching(query as CFDictionary, &result)
    guard status == errSecSuccess, let data = result as? Data else { return nil }
    return String(data: data, encoding: .utf8)
}
```

---

## Build & Distribution

### 1. Xcode Project Structure

```
AxiomFS.xcodeproj/
├── AxiomFS/                    # Menu bar app target
│   ├── AxiomFSApp.swift
│   ├── ContentView.swift
│   ├── SettingsView.swift
│   ├── ConnectionState.swift
│   └── Assets.xcassets
├── AxiomProvider/              # File Provider extension target
│   ├── Extension.swift
│   ├── Enumerator.swift
│   ├── Item.swift
│   └── Info.plist
└── AxiomKit/                   # Shared Swift framework
    ├── AxiomClient.swift       # HTTP API client
    ├── Configuration.swift     # ~/.axiom.toml parsing
    ├── Models.swift            # Dataset, QueryResult, etc.
    ├── QueryBuilder.swift      # APL query construction
    └── Cache.swift             # Response caching
```

### 2. Code Signing

**Required Certificates**:
- Developer ID Application (for direct distribution)
- Developer ID Installer (for PKG, if needed)

**Entitlements** (`App.entitlements`):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "...">
<plist version="1.0">
<dict>
    <key>com.apple.security.app-sandbox</key>
    <true/>
    <key>com.apple.security.application-groups</key>
    <array>
        <string>group.com.axiom.fs</string>
    </array>
    <!-- Keychain Sharing - different from App Groups! -->
    <key>keychain-access-groups</key>
    <array>
        <string>$(AppIdentifierPrefix)com.axiom.fs</string>
    </array>
    <key>com.apple.security.network.client</key>
    <true/>
    <!-- Removed: files.user-selected.read-write - not needed for File Provider -->
</dict>
</plist>
```

**Extension Entitlements** (`Provider.entitlements`):
```xml
<dict>
    <key>com.apple.security.app-sandbox</key>
    <true/>
    <key>com.apple.security.application-groups</key>
    <array>
        <string>group.com.axiom.fs</string>
    </array>
    <!-- Keychain Sharing - must match main app -->
    <key>keychain-access-groups</key>
    <array>
        <string>$(AppIdentifierPrefix)com.axiom.fs</string>
    </array>
    <key>com.apple.security.network.client</key>
    <true/>
</dict>
```

> **Note:** Enable the **File Provider** capability in Xcode for the extension target.
> Xcode will add the required File Provider entitlements automatically.
> Do not hand-edit File Provider entitlements—it causes "works locally, fails elsewhere" issues.

### 3. Hardened Runtime

Required for notarization. Enable in Xcode:
- Target → Signing & Capabilities → + Hardened Runtime

Key options:
- Allow Execution of JIT-compiled Code: **NO**
- Allow Unsigned Executable Memory: **NO** (unless Go needs it)
- Disable Library Validation: **YES** (for Go dylib if not signed)

### 4. Notarization Process

```bash
# 1. Archive in Xcode or xcodebuild
xcodebuild -scheme "Axiom FS" -archivePath build/AxiomFS.xcarchive archive

# 2. Export signed app
xcodebuild -exportArchive \
    -archivePath build/AxiomFS.xcarchive \
    -exportPath build/export \
    -exportOptionsPlist ExportOptions.plist

# 3. Create DMG
create-dmg \
    --volname "Axiom FS" \
    --window-size 600 400 \
    --icon-size 100 \
    --icon "Axiom FS.app" 150 200 \
    --app-drop-link 450 200 \
    "build/AxiomFS.dmg" \
    "build/export/Axiom FS.app"

# 4. Sign DMG
codesign --sign "Developer ID Application: TEAM" \
    --timestamp \
    build/AxiomFS.dmg

# 5. Notarize
xcrun notarytool submit build/AxiomFS.dmg \
    --apple-id "dev@axiom.co" \
    --team-id "TEAMID" \
    --password "@keychain:AC_PASSWORD" \
    --wait

# 6. Staple
xcrun stapler staple build/AxiomFS.dmg
```

### 5. DMG Customization

Using [create-dmg](https://github.com/create-dmg/create-dmg):
```bash
brew install create-dmg

create-dmg \
    --volname "Axiom FS" \
    --volicon "resources/axiom.icns" \
    --background "resources/dmg-background.png" \
    --window-pos 200 120 \
    --window-size 600 400 \
    --icon-size 100 \
    --icon "Axiom FS.app" 150 185 \
    --hide-extension "Axiom FS.app" \
    --app-drop-link 450 185 \
    "AxiomFS-1.0.dmg" \
    "build/Axiom FS.app"
```

---

## Implementation Phases

### Phase 1: AxiomKit Framework
- [ ] Create Swift package for shared code
- [ ] Implement `AxiomClient` with async/await HTTP calls
- [ ] Add `~/.axiom.toml` config parsing
- [ ] Response caching

### Phase 2: Menu Bar App
- [ ] Create Xcode project with SwiftUI `MenuBarExtra`
- [ ] Preferences window (credentials, settings)
- [ ] Keychain storage with **Keychain Sharing**
- [ ] Launch-at-login (`SMAppService`)
- [ ] Connection status indicator

### Phase 3: File Provider Extension
- [ ] Create File Provider extension target
- [ ] Implement `NSFileProviderReplicatedExtension`
- [ ] Metadata store for stable item identifiers
- [ ] `NSFileProviderEnumerator` for datasets
- [ ] Content fetching with local file materialization
- [ ] Write support: `createItem`/`modifyItem`/`deleteItem` for `_queries`

### Phase 4: Polish & Distribution
- [ ] Code sign with Developer ID
- [ ] Notarize and create DMG

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| File Provider API complexity | Start with Apple's FruitBasket sample; iterate |
| Sandboxing restrictions | Test early; use App Groups for shared data |
| Query execution latency | Cache aggressively; Finder shows spinners for dataless files |
| macOS version compatibility | Target macOS 13+ for MenuBarExtra and SMAppService |
| API rate limits | Implement caching and request deduplication |
| Finder enumeration storms | Aggressive metadata caching; avoid network calls during enumeration |
| Auth failures look like broken drive | Clear "Reconnect" path in app; map errors to user-actionable states |
| Virtual query tree becomes unmaintainable | Constrain exposed filesystem to small stable hierarchy |

---

## Alternative: Keep NFS + Add Menu Bar App

If File Provider proves too complex, a simpler approach:

1. **Menu bar app** starts/stops Go NFS server
2. **Auto-mount** NFS share at login
3. Uses existing axiom-fs code unchanged
4. Less native but functional

```swift
// Mount NFS programmatically
let task = Process()
task.executableURL = URL(fileURLWithPath: "/sbin/mount")
task.arguments = ["-t", "nfs", "localhost:/", "/Volumes/Axiom"]
try task.run()
```

---

## References

- [Apple File Provider Documentation](https://developer.apple.com/documentation/fileprovider)
- [FruitBasket Sample Code](https://developer.apple.com/documentation/fileprovider/synchronizing-files-using-file-provider-extensions)
- [NSFileProviderReplicatedExtension](https://developer.apple.com/documentation/fileprovider/nsfileproviderreplicatedextension)
- [MenuBarExtra (SwiftUI)](https://developer.apple.com/documentation/swiftui/menubarextra)
- [Notarizing macOS Software](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution)
- [Creating XPC Services](https://developer.apple.com/documentation/xpc/creating-xpc-services)
- [Dropbox File Provider Migration](https://help.dropbox.com/installs/dropbox-for-macos-support)
