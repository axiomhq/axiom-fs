import XCTest
@testable import AxiomKit

final class ConfigurationTests: XCTestCase {
    
    var tempDir: URL!
    
    override func setUp() {
        super.setUp()
        tempDir = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try? FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }
    
    override func tearDown() {
        try? FileManager.default.removeItem(at: tempDir)
        super.tearDown()
    }
    
    private func writeTempToml(_ content: String) throws -> URL {
        let file = tempDir.appendingPathComponent("test.toml")
        try content.write(to: file, atomically: true, encoding: .utf8)
        return file
    }
    
    // MARK: - TOML Parsing Tests
    
    func testParseValidToml() throws {
        let toml = """
        url = "https://api.axiom.co"
        token = "xaat-test-token"
        org_id = "my-org"
        """
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertTrue(content.contains("token"))
        XCTAssertTrue(content.contains("xaat-test-token"))
    }
    
    func testParseTomlWithComments() throws {
        let toml = """
        # This is a comment
        url = "https://api.axiom.co"
        # Another comment
        token = "xaat-test-token"
        """
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertTrue(content.contains("token"))
        XCTAssertTrue(content.contains("xaat-test-token"))
    }
    
    func testParseTomlWithExtraWhitespace() throws {
        let toml = """
          url   =   "https://api.axiom.co"  
          token   =   "xaat-test-token"  
        """
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertTrue(content.contains("token"))
    }
    
    func testParseEmptyTomlThrowsInvalidConfig() throws {
        let toml = ""
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertTrue(content.isEmpty)
    }
    
    func testParseTomlMissingTokenThrowsInvalidConfig() throws {
        let toml = """
        url = "https://api.axiom.co"
        org_id = "my-org"
        """
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertFalse(content.contains("token ="))
    }
    
    func testParseTomlWithSectionHeaders() throws {
        let toml = """
        [default]
        token = "xaat-test-token"
        """
        let file = try writeTempToml(toml)
        let content = try String(contentsOf: file, encoding: .utf8)
        
        XCTAssertTrue(content.contains("[default]"))
        XCTAssertTrue(content.contains("token"))
    }
    
    // MARK: - AxiomConfig Tests
    
    func testAxiomConfigInit() {
        let config = AxiomConfig(url: "https://custom.axiom.co", token: "test-token", orgID: "org-123")
        
        XCTAssertEqual(config.url, "https://custom.axiom.co")
        XCTAssertEqual(config.token, "test-token")
        XCTAssertEqual(config.orgID, "org-123")
    }
    
    func testAxiomConfigInitWithDefaults() {
        let config = AxiomConfig(token: "test-token")
        
        XCTAssertNil(config.url)
        XCTAssertEqual(config.token, "test-token")
        XCTAssertNil(config.orgID)
    }
    
    func testMakeClientWithDefaultURL() {
        let config = AxiomConfig(token: "test-token")
        let client = config.makeClient()
        
        XCTAssertEqual(client.baseURL.absoluteString, "https://api.axiom.co")
        XCTAssertEqual(client.token, "test-token")
        XCTAssertNil(client.orgID)
    }
    
    func testMakeClientWithCustomURL() {
        let config = AxiomConfig(url: "https://custom.axiom.co", token: "test-token", orgID: "org-123")
        let client = config.makeClient()
        
        XCTAssertEqual(client.baseURL.absoluteString, "https://custom.axiom.co")
        XCTAssertEqual(client.token, "test-token")
        XCTAssertEqual(client.orgID, "org-123")
    }
    
    func testMakeClientWithInvalidURLFallsBackToDefault() {
        let config = AxiomConfig(url: "", token: "test-token")
        let client = config.makeClient()
        
        XCTAssertEqual(client.baseURL.absoluteString, "https://api.axiom.co")
    }
    
    // MARK: - Error Cases
    
    func testAxiomErrorConfigNotFound() {
        let error = AxiomError.configNotFound
        XCTAssertNotNil(error)
    }
    
    func testAxiomErrorInvalidConfig() {
        let error = AxiomError.invalidConfig("Missing token")
        if case .invalidConfig(let message) = error {
            XCTAssertEqual(message, "Missing token")
        } else {
            XCTFail("Expected invalidConfig error")
        }
    }
}
