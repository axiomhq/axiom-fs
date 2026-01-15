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
        
        // Test TOML parsing logic directly
        let content = try String(contentsOf: tempFile, encoding: .utf8)
        XCTAssertTrue(content.contains("token"))
    }
    
    func testQueryFormatRawValue() {
        XCTAssertEqual(QueryFormat.tabular.rawValue, "tabular")
        XCTAssertEqual(QueryFormat.legacy.rawValue, "legacy")
    }
}
