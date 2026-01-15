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
        }
        .frame(width: 450, height: 250)
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
        // Save to Keychain
        // KeychainManager.shared.saveToken(token, for: appState.axiomURL)
    }
}

#Preview {
    SettingsView()
        .environmentObject(AppState())
}
