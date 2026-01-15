import Foundation

public struct AxiomConfig: Sendable {
    public let url: String?
    public let token: String
    public let orgID: String?
    
    public init(url: String? = nil, token: String, orgID: String? = nil) {
        self.url = url
        self.token = token
        self.orgID = orgID
    }
    
    public static func load() throws -> AxiomConfig {
        // Try environment variables first
        if let token = ProcessInfo.processInfo.environment["AXIOM_TOKEN"] {
            return AxiomConfig(
                url: ProcessInfo.processInfo.environment["AXIOM_URL"],
                token: token,
                orgID: ProcessInfo.processInfo.environment["AXIOM_ORG_ID"]
            )
        }
        
        // Try ~/.axiom.toml
        let home = FileManager.default.homeDirectoryForCurrentUser
        let configPath = home.appendingPathComponent(".axiom.toml")
        
        guard FileManager.default.fileExists(atPath: configPath.path) else {
            throw AxiomError.configNotFound
        }
        
        let content = try String(contentsOf: configPath, encoding: .utf8)
        return try parse(toml: content)
    }
    
    private static func parse(toml content: String) throws -> AxiomConfig {
        var url: String?
        var token: String?
        var orgID: String?
        
        for line in content.components(separatedBy: .newlines) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#"), !trimmed.hasPrefix("[") else { continue }
            
            let parts = trimmed.components(separatedBy: "=")
            guard parts.count == 2 else { continue }
            
            let key = parts[0].trimmingCharacters(in: .whitespaces)
            var value = parts[1].trimmingCharacters(in: .whitespaces)
            
            // Remove quotes
            if value.hasPrefix("\"") && value.hasSuffix("\"") {
                value = String(value.dropFirst().dropLast())
            }
            
            switch key {
            case "url": url = value
            case "token": token = value
            case "org_id": orgID = value
            default: break
            }
        }
        
        guard let token else {
            throw AxiomError.invalidConfig("Missing 'token' in ~/.axiom.toml")
        }
        
        return AxiomConfig(url: url, token: token, orgID: orgID)
    }
    
    public func makeClient() -> AxiomClient {
        let baseURL = url.flatMap { URL(string: $0) } ?? URL(string: "https://api.axiom.co")!
        return AxiomClient(url: baseURL, token: token, orgID: orgID)
    }
}
