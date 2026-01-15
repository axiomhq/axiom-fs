import SwiftUI
import FileProvider

@MainActor
class AppState: ObservableObject {
    enum ConnectionStatus {
        case disconnected
        case connecting
        case connected
        case error(String)
    }
    
    @Published var status: ConnectionStatus = .disconnected
    @Published var datasets: [String] = []
    @Published var lastError: String?
    
    @AppStorage("axiomURL") var axiomURL: String = "https://api.axiom.co"
    @AppStorage("axiomOrgID") var axiomOrgID: String = ""
    
    private var domain: NSFileProviderDomain?
    
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
        case .connecting: return "Connecting..."
        case .connected: return "Connected"
        case .error(let msg): return "Error: \(msg)"
        }
    }
    
    var isConnected: Bool {
        if case .connected = status { return true }
        return false
    }
    
    init() {
        // Auto-connect on launch if credentials exist
        Task {
            await autoConnect()
        }
    }
    
    func autoConnect() async {
        // Check if we have credentials in Keychain
        // For now, try to load from ~/.axiom.toml
        do {
            try await connect()
        } catch {
            // Silent fail on auto-connect
        }
    }
    
    func connect() async throws {
        status = .connecting
        
        do {
            // Register File Provider domain
            let domainID = NSFileProviderDomainIdentifier(rawValue: "com.axiom.fs")
            let newDomain = NSFileProviderDomain(identifier: domainID, displayName: "Axiom")
            
            // Remove existing domain first
            if let existing = try? await NSFileProviderManager.domains().first(where: { $0.identifier == domainID }) {
                try await NSFileProviderManager.remove(existing)
            }
            
            try await NSFileProviderManager.add(newDomain)
            self.domain = newDomain
            
            status = .connected
        } catch {
            status = .error(error.localizedDescription)
            throw error
        }
    }
    
    func disconnect() async {
        if let domain {
            try? await NSFileProviderManager.remove(domain)
            self.domain = nil
        }
        status = .disconnected
    }
    
    func openInFinder() {
        guard let domain else { return }
        
        if let manager = NSFileProviderManager(for: domain) {
            manager.getUserVisibleURL(for: .rootContainer) { url, error in
                if let url {
                    NSWorkspace.shared.open(url)
                }
            }
        }
    }
}
