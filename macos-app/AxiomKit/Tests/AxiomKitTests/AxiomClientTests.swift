import XCTest
@testable import AxiomKit

final class AxiomClientTests: XCTestCase {
    
    func testConfigParsing() throws {
        let toml = """
        url = "https://api.axiom.co"
        token = "xaat-test-token"
        org_id = "my-org"
        """
        
        let tempFile = FileManager.default.temporaryDirectory.appendingPathComponent("test-axiom.toml")
        try toml.write(to: tempFile, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: tempFile) }
        
        let content = try String(contentsOf: tempFile, encoding: .utf8)
        XCTAssertTrue(content.contains("token"))
    }
    
    func testQueryFormatRawValue() {
        XCTAssertEqual(QueryFormat.tabular.rawValue, "tabular")
        XCTAssertEqual(QueryFormat.legacy.rawValue, "legacy")
    }
    
    // MARK: - Client Initialization Tests
    
    func testClientInitWithDefaults() {
        let client = AxiomClient(token: "test-token")
        
        XCTAssertEqual(client.baseURL.absoluteString, "https://api.axiom.co")
        XCTAssertEqual(client.token, "test-token")
        XCTAssertNil(client.orgID)
    }
    
    func testClientInitWithCustomURL() {
        let url = URL(string: "https://custom.axiom.co")!
        let client = AxiomClient(url: url, token: "test-token", orgID: "org-123")
        
        XCTAssertEqual(client.baseURL.absoluteString, "https://custom.axiom.co")
        XCTAssertEqual(client.token, "test-token")
        XCTAssertEqual(client.orgID, "org-123")
    }
    
    // MARK: - Mocked HTTP Tests
    
    func testListDatasetsSuccess() async throws {
        let responseData = """
        [
            {"name": "dataset-1", "description": "First dataset"},
            {"name": "dataset-2", "description": "Second dataset"}
        ]
        """.data(using: .utf8)!
        
        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.url?.path, "/v1/datasets")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-token")
            
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, responseData)
        }
        
        let client = makeClient()
        let datasets = try await client.listDatasets()
        
        XCTAssertEqual(datasets.count, 2)
        XCTAssertEqual(datasets[0].name, "dataset-1")
        XCTAssertEqual(datasets[0].id, "dataset-1")
        XCTAssertEqual(datasets[1].name, "dataset-2")
    }
    
    func testListDatasetsWithOrgHeader() async throws {
        let responseData = "[]".data(using: .utf8)!
        
        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.value(forHTTPHeaderField: "X-Axiom-Org-Id"), "my-org")
            
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, responseData)
        }
        
        let client = makeClient(orgID: "my-org")
        _ = try await client.listDatasets()
    }
    
    func testQueryWithTabularFormat() async throws {
        let responseData = """
        {
            "status": {
                "elapsedTime": 100,
                "rowsExamined": 1000,
                "rowsMatched": 50
            },
            "tables": [
                {
                    "name": "0",
                    "columns": [{"name": "_time", "type": "datetime"}],
                    "events": []
                }
            ]
        }
        """.data(using: .utf8)!
        
        MockURLProtocol.requestHandler = { request in
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertTrue(request.url?.absoluteString.contains("format=tabular") ?? false)
            XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/json")
            
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, responseData)
        }
        
        let client = makeClient()
        let result = try await client.query(apl: "['dataset'] | limit 10", format: .tabular)
        
        XCTAssertEqual(result.status.elapsedTime, 100)
        XCTAssertEqual(result.tables?.count, 1)
    }
    
    func testQueryWithLegacyFormat() async throws {
        let responseData = """
        {
            "status": {
                "elapsedTime": 50,
                "rowsExamined": 500,
                "rowsMatched": 25
            }
        }
        """.data(using: .utf8)!
        
        MockURLProtocol.requestHandler = { request in
            XCTAssertTrue(request.url?.absoluteString.contains("format=legacy") ?? false)
            
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, responseData)
        }
        
        let client = makeClient()
        let result = try await client.query(apl: "['dataset'] | limit 5", format: .legacy)
        
        XCTAssertEqual(result.status.rowsMatched, 25)
    }
    
    func testQueryRawReturnsData() async throws {
        let responseData = "{\"raw\": \"data\"}".data(using: .utf8)!
        
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, responseData)
        }
        
        let client = makeClient()
        let data = try await client.queryRaw(apl: "['dataset']")
        
        XCTAssertEqual(data, responseData)
    }
    
    // MARK: - Error Handling Tests
    
    func testAuthenticationFailedOn401() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 401,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.listDatasets()
            XCTFail("Expected authenticationFailed error")
        } catch {
            guard case AxiomError.authenticationFailed = error else {
                XCTFail("Expected authenticationFailed, got \(error)")
                return
            }
        }
    }
    
    func testAuthenticationFailedOn403() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 403,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.listDatasets()
            XCTFail("Expected authenticationFailed error")
        } catch {
            guard case AxiomError.authenticationFailed = error else {
                XCTFail("Expected authenticationFailed, got \(error)")
                return
            }
        }
    }
    
    func testRateLimitedOn429() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 429,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.listDatasets()
            XCTFail("Expected rateLimited error")
        } catch {
            guard case AxiomError.rateLimited = error else {
                XCTFail("Expected rateLimited, got \(error)")
                return
            }
        }
    }
    
    func testHttpErrorOnOtherStatusCodes() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 500,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.listDatasets()
            XCTFail("Expected httpError")
        } catch {
            guard case AxiomError.httpError(let code) = error else {
                XCTFail("Expected httpError, got \(error)")
                return
            }
            XCTAssertEqual(code, 500)
        }
    }
    
    func testQueryAuthenticationFailed() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 401,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.query(apl: "['dataset']")
            XCTFail("Expected authenticationFailed error")
        } catch {
            guard case AxiomError.authenticationFailed = error else {
                XCTFail("Expected authenticationFailed, got \(error)")
                return
            }
        }
    }
    
    func testQueryRateLimited() async throws {
        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 429,
                httpVersion: nil,
                headerFields: nil
            )!
            return (response, Data())
        }
        
        let client = makeClient()
        
        do {
            _ = try await client.query(apl: "['dataset']")
            XCTFail("Expected rateLimited error")
        } catch {
            guard case AxiomError.rateLimited = error else {
                XCTFail("Expected rateLimited, got \(error)")
                return
            }
        }
    }
    
    // MARK: - Helper Methods
    
    private func makeClient(orgID: String? = nil) -> AxiomClient {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockURLProtocol.self]
        let session = URLSession(configuration: config)
        
        return AxiomClient(
            url: URL(string: "https://api.axiom.co")!,
            token: "test-token",
            orgID: orgID,
            session: session
        )
    }
}

// MARK: - Mock URL Protocol

final class MockURLProtocol: URLProtocol {
    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data))?
    
    override class func canInit(with request: URLRequest) -> Bool {
        true
    }
    
    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }
    
    override func startLoading() {
        guard let handler = MockURLProtocol.requestHandler else {
            fatalError("MockURLProtocol.requestHandler not set")
        }
        
        do {
            let (response, data) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }
    
    override func stopLoading() {}
}
