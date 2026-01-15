import SwiftUI

struct MenuBarView: View {
    @EnvironmentObject var appState: AppState
    
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header
            HStack(spacing: 8) {
                Circle()
                    .fill(statusColor)
                    .frame(width: 8, height: 8)
                Text("Axiom FS")
                    .fontWeight(.semibold)
                Spacer()
                Text("NFS")
                    .font(.caption2)
                    .foregroundColor(.secondary)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.secondary.opacity(0.2))
                    .cornerRadius(4)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 10)
            
            Divider()
            
            // Status
            VStack(alignment: .leading, spacing: 4) {
                Text(appState.statusText)
                    .font(.caption)
                    .foregroundColor(.secondary)
                
                if case .running(let pid) = appState.nfsManager.state {
                    Text("PID: \(pid)")
                        .font(.caption2)
                        .foregroundColor(.secondary.opacity(0.7))
                }
                
                if !appState.hasBinary {
                    Text("⚠️ axiom-fs not found")
                        .font(.caption)
                        .foregroundColor(.orange)
                }
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)
            
            Divider()
            
            // Actions
            VStack(alignment: .leading, spacing: 0) {
                if appState.isConnected {
                    MenuButton(title: "Open in Finder", icon: "folder") {
                        appState.openInFinder()
                    }
                    MenuButton(title: "Disconnect", icon: "eject") {
                        Task { await appState.disconnect() }
                    }
                } else {
                    MenuButton(title: "Connect", icon: "link", disabled: !appState.hasBinary) {
                        Task { try? await appState.connect() }
                    }
                }
            }
            
            Divider()
            
            // Footer
            VStack(alignment: .leading, spacing: 0) {
                MenuButton(title: "Settings...", icon: "gear") {
                    if #available(macOS 14.0, *) {
                        NSApp.sendAction(Selector(("showSettingsWindow:")), to: nil, from: nil)
                    } else {
                        NSApp.sendAction(Selector(("showPreferencesWindow:")), to: nil, from: nil)
                    }
                }
                MenuButton(title: "Quit Axiom FS", icon: "power") {
                    Task {
                        await appState.disconnect()
                        NSApplication.shared.terminate(nil)
                    }
                }
            }
        }
        .frame(width: 220)
    }
    
    private var statusColor: Color {
        switch appState.status {
        case .connected: return .green
        case .connecting: return .orange
        case .disconnected: return .gray
        case .error: return .red
        }
    }
}

struct MenuButton: View {
    let title: String
    let icon: String
    var disabled: Bool = false
    let action: () -> Void
    
    @State private var isHovered = false
    
    var body: some View {
        Button(action: action) {
            HStack(spacing: 8) {
                Image(systemName: icon)
                    .frame(width: 16)
                Text(title)
                Spacer()
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .background(isHovered && !disabled ? Color.accentColor.opacity(0.2) : Color.clear)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .disabled(disabled)
        .opacity(disabled ? 0.5 : 1)
        .onHover { isHovered = $0 }
    }
}
