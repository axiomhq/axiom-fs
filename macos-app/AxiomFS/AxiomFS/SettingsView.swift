import SwiftUI
import ServiceManagement

struct SettingsView: View {
    @EnvironmentObject var appState: AppState
    @State private var token: String = ""
    @State private var launchAtLogin: Bool = false

    var body: some View {
        TabView {
            GeneralSettingsView(launchAtLogin: $launchAtLogin)
                .tabItem {
                    Label("General", systemImage: "gear")
                }

            AccountSettingsView(token: $token)
                .environmentObject(appState)
                .tabItem {
                    Label("Account", systemImage: "person.crop.circle")
                }

            NFSSettingsView()
                .environmentObject(appState)
                .tabItem {
                    Label("NFS", systemImage: "externaldrive.connected.to.line.below")
                }
        }
        .frame(width: 450, height: 280)
        .onAppear {
            launchAtLogin = SMAppService.mainApp.status == .enabled
        }
    }
}

struct GeneralSettingsView: View {
    @Binding var launchAtLogin: Bool

    var body: some View {
        Form {
            Toggle("Launch at Login", isOn: $launchAtLogin)
                .onChange(of: launchAtLogin) { newValue in
                    do {
                        if newValue {
                            try SMAppService.mainApp.register()
                        } else {
                            try SMAppService.mainApp.unregister()
                        }
                    } catch {
                        print("Failed to update launch at login: \(error)")
                    }
                }
        }
        .padding()
    }
}

struct AccountSettingsView: View {
    @EnvironmentObject var appState: AppState
    @Binding var token: String

    var body: some View {
        Form {
            TextField("Axiom URL", text: $appState.axiomURL)

            SecureField("API Token", text: $token)
                .onSubmit {
                    saveToken()
                }

            TextField("Organization ID (optional)", text: $appState.axiomOrgID)

            Text("Token is read from AXIOM_TOKEN environment variable or ~/.axiom.toml")
                .font(.caption)
                .foregroundColor(.secondary)

            HStack {
                Spacer()
                Button("Save") {
                    saveToken()
                }
                .buttonStyle(.borderedProminent)
            }
        }
        .padding()
    }

    private func saveToken() {
        // TODO: Save to Keychain via KeychainManager
        // For now, users should set AXIOM_TOKEN or use ~/.axiom.toml
    }
}

struct NFSSettingsView: View {
    @EnvironmentObject var appState: AppState

    var body: some View {
        Form {
            Section {
                HStack {
                    Text("Binary Location")
                    Spacer()
                    if let path = appState.nfsManager.binaryPath {
                        Text(path)
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .lineLimit(1)
                            .truncationMode(.middle)
                    } else {
                        Text("Not found")
                            .font(.caption)
                            .foregroundColor(.red)
                    }
                }

                HStack {
                    Text("Mount Point")
                    Spacer()
                    Text("~/Axiom")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                HStack {
                    Text("Server Address")
                    Spacer()
                    Text("127.0.0.1:12049")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            Section {
                if !appState.hasBinary {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Install axiom-fs")
                            .font(.headline)

                        Text("The NFS server binary was not found. Install it with:")
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Text("go install github.com/axiomhq/axiom-fs/cmd/axiom-fs@latest")
                            .font(.system(.caption, design: .monospaced))
                            .textSelection(.enabled)
                            .padding(8)
                            .background(Color.secondary.opacity(0.1))
                            .cornerRadius(4)
                    }
                }
            }

            if !appState.nfsManager.serverOutput.isEmpty {
                Section("Server Output") {
                    ScrollView {
                        Text(appState.nfsManager.serverOutput)
                            .font(.system(.caption, design: .monospaced))
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .frame(height: 80)
                }
            }
        }
        .padding()
    }
}

#Preview {
    SettingsView()
        .environmentObject(AppState())
}
