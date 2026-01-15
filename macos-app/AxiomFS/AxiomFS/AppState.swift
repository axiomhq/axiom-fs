import SwiftUI

@MainActor
class AppState: ObservableObject {
    enum ConnectionStatus: Equatable {
        case disconnected
        case connecting
        case connected
        case error(String)
        
        static func == (lhs: ConnectionStatus, rhs: ConnectionStatus) -> Bool {
            switch (lhs, rhs) {
            case (.disconnected, .disconnected),
                 (.connecting, .connecting),
                 (.connected, .connected):
                return true
            case (.error(let a), .error(let b)):
                return a == b
            default:
                return false
            }
        }
    }
    
    @Published var status: ConnectionStatus = .disconnected
    @Published var datasets: [String] = []
    
    @AppStorage("axiomURL") var axiomURL: String = "https://api.axiom.co"
    @AppStorage("axiomOrgID") var axiomOrgID: String = ""
    
    let nfsManager = NFSProcessManager()
    
    var statusIcon: String {
        switch status {
        case .disconnected: return "externaldrive.badge.xmark"
        case .connecting: return "externaldrive.badge.questionmark"
        case .connected: return "externaldrive.fill.badge.checkmark"
        case .error: return "externaldrive.badge.exclamationmark"
        }
    }
    
    var statusText: String {
        switch status {
        case .disconnected: return "Disconnected"
        case .connecting: return "Starting NFS server..."
        case .connected: return "Connected (NFS)"
        case .error(let msg): return "Error: \(msg)"
        }
    }
    
    var isConnected: Bool {
        if case .connected = status { return true }
        return false
    }
    
    var hasBinary: Bool {
        nfsManager.binaryPath != nil
    }
    
    init() {
        // Sync NFS manager state changes to our status
        Task {
            for await _ in nfsManager.$state.values {
                await MainActor.run {
                    self.syncStatus()
                }
            }
        }
    }
    
    private func syncStatus() {
        switch nfsManager.state {
        case .stopped:
            if case .connecting = status {
                // Don't override if we just started
            } else {
                status = .disconnected
            }
        case .starting:
            status = .connecting
        case .running:
            status = .connected
        case .error(let msg):
            status = .error(msg)
        }
    }
    
    func connect() async throws {
        status = .connecting
        
        do {
            try await nfsManager.start()
            status = .connected
        } catch {
            status = .error(error.localizedDescription)
            throw error
        }
    }
    
    func disconnect() async {
        await nfsManager.stop()
        status = .disconnected
    }
    
    func openInFinder() {
        nfsManager.openInFinder()
    }
}
