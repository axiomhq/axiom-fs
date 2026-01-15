import SwiftUI

struct MenuBarView: View {
    @EnvironmentObject var appState: AppState
    
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Header
            HStack {
                Image(systemName: appState.statusIcon)
                    .foregroundColor(statusColor)
                Text("Axiom FS")
                    .font(.headline)
                Spacer()
            }
            .padding(.horizontal)
            .padding(.top, 8)
            
            Divider()
            
            // Status
            HStack {
                Circle()
                    .fill(statusColor)
                    .frame(width: 8, height: 8)
                Text(appState.statusText)
                    .font(.subheadline)
                    .foregroundColor(.secondary)
            }
            .padding(.horizontal)
            
            Divider()
            
            // Actions
            if appState.isConnected {
                Button("Open in Finder") {
                    appState.openInFinder()
                }
                .buttonStyle(.plain)
                .padding(.horizontal)
                
                Button("Disconnect") {
                    Task {
                        await appState.disconnect()
                    }
                }
                .buttonStyle(.plain)
                .padding(.horizontal)
            } else {
                Button("Connect") {
                    Task {
                        try? await appState.connect()
                    }
                }
                .buttonStyle(.plain)
                .padding(.horizontal)
            }
            
            Divider()
            
            // Footer
            HStack {
                Button("Settings...") {
                    NSApp.sendAction(Selector(("showSettingsWindow:")), to: nil, from: nil)
                }
                .buttonStyle(.plain)
                
                Spacer()
                
                Button("Quit") {
                    NSApplication.shared.terminate(nil)
                }
                .buttonStyle(.plain)
            }
            .padding(.horizontal)
            .padding(.bottom, 8)
        }
        .frame(width: 240)
    }
    
    private var statusColor: Color {
        switch appState.status {
        case .connected: return .green
        case .connecting: return .yellow
        case .disconnected: return .gray
        case .error: return .red
        }
    }
}
