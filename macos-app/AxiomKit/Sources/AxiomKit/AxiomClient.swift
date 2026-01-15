import Foundation

public class AxiomClient {
    public let baseURL: URL
    public let token: String
    public let orgID: String?
    
    private let session: URLSession
    private let cache = ResponseCache()
    
    public init(
        url: URL = URL(string: "https://api.axiom.co")!,
        token: String,
        orgID: String? = nil,
        session: URLSession = .shared
    ) {
        self.baseURL = url
        self.token = token
        self.orgID = orgID
        self.session = session
    }
    
    public func listDatasets() async throws -> [Dataset] {
        let url = baseURL.appendingPathComponent("v1/datasets")
        var request = URLRequest(url: url)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        if let orgID { request.setValue(orgID, forHTTPHeaderField: "X-Axiom-Org-Id") }
        
        let (data, response) = try await session.data(for: request)
        try validateResponse(response)
        return try JSONDecoder().decode([Dataset].self, from: data)
    }
    
    public func query(apl: String, format: QueryFormat = .tabular) async throws -> QueryResult {
        var components = URLComponents(url: baseURL.appendingPathComponent("v1/datasets/_apl"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "format", value: format.rawValue)]
        
        var request = URLRequest(url: components.url!)
        request.httpMethod = "POST"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let orgID { request.setValue(orgID, forHTTPHeaderField: "X-Axiom-Org-Id") }
        
        let body = QueryRequest(apl: apl)
        request.httpBody = try JSONEncoder().encode(body)
        
        let (data, response) = try await session.data(for: request)
        try validateResponse(response)
        return try JSONDecoder().decode(QueryResult.self, from: data)
    }
    
    public func queryRaw(apl: String, format: QueryFormat = .tabular) async throws -> Data {
        var components = URLComponents(url: baseURL.appendingPathComponent("v1/datasets/_apl"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "format", value: format.rawValue)]
        
        var request = URLRequest(url: components.url!)
        request.httpMethod = "POST"
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let orgID { request.setValue(orgID, forHTTPHeaderField: "X-Axiom-Org-Id") }
        
        let body = QueryRequest(apl: apl)
        request.httpBody = try JSONEncoder().encode(body)
        
        let (data, response) = try await session.data(for: request)
        try validateResponse(response)
        return data
    }
    
    private func validateResponse(_ response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else { return }
        switch http.statusCode {
        case 200..<300: return
        case 401, 403: throw AxiomError.authenticationFailed
        case 429: throw AxiomError.rateLimited
        default: throw AxiomError.httpError(http.statusCode)
        }
    }
}

private struct QueryRequest: Encodable {
    let apl: String
}
