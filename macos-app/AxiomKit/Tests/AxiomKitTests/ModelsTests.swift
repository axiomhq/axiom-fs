import XCTest
@testable import AxiomKit

final class ModelsTests: XCTestCase {
    
    // MARK: - Dataset Tests
    
    func testDatasetDecodingUsesNameAsId() throws {
        let json = """
        {
            "name": "my-dataset",
            "description": "Test dataset"
        }
        """.data(using: .utf8)!
        
        let dataset = try JSONDecoder().decode(Dataset.self, from: json)
        
        XCTAssertEqual(dataset.id, "my-dataset")
        XCTAssertEqual(dataset.name, "my-dataset")
        XCTAssertEqual(dataset.description, "Test dataset")
        XCTAssertNil(dataset.created)
    }
    
    func testDatasetDecodingWithoutDescription() throws {
        let json = """
        {
            "name": "minimal-dataset"
        }
        """.data(using: .utf8)!
        
        let dataset = try JSONDecoder().decode(Dataset.self, from: json)
        
        XCTAssertEqual(dataset.id, "minimal-dataset")
        XCTAssertEqual(dataset.name, "minimal-dataset")
        XCTAssertNil(dataset.description)
    }
    
    func testDatasetDecodingWithCreatedDate() throws {
        let json = """
        {
            "name": "dated-dataset",
            "created": 1704067200
        }
        """.data(using: .utf8)!
        
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .secondsSince1970
        let dataset = try decoder.decode(Dataset.self, from: json)
        
        XCTAssertEqual(dataset.name, "dated-dataset")
        XCTAssertNotNil(dataset.created)
    }
    
    // MARK: - QueryResult Tests
    
    func testQueryResultDecodingWithEmptyTables() throws {
        let json = """
        {
            "status": {
                "elapsedTime": 100,
                "rowsExamined": 1000,
                "rowsMatched": 50
            },
            "tables": []
        }
        """.data(using: .utf8)!
        
        let result = try JSONDecoder().decode(QueryResult.self, from: json)
        
        XCTAssertEqual(result.status.elapsedTime, 100)
        XCTAssertEqual(result.status.rowsExamined, 1000)
        XCTAssertEqual(result.status.rowsMatched, 50)
        XCTAssertEqual(result.tables?.count, 0)
    }
    
    func testQueryResultDecodingWithNullTables() throws {
        let json = """
        {
            "status": {
                "elapsedTime": 50,
                "rowsExamined": 500,
                "rowsMatched": 25
            }
        }
        """.data(using: .utf8)!
        
        let result = try JSONDecoder().decode(QueryResult.self, from: json)
        
        XCTAssertNil(result.tables)
    }
    
    func testQueryResultDecodingWithPopulatedTables() throws {
        let json = """
        {
            "status": {
                "elapsedTime": 200,
                "rowsExamined": 5000,
                "rowsMatched": 100
            },
            "tables": [
                {
                    "name": "0",
                    "columns": [
                        {"name": "_time", "type": "datetime"},
                        {"name": "message", "type": "string"}
                    ],
                    "events": [
                        {"_time": "2024-01-01T00:00:00Z", "message": "test event"}
                    ]
                }
            ]
        }
        """.data(using: .utf8)!
        
        let result = try JSONDecoder().decode(QueryResult.self, from: json)
        
        XCTAssertEqual(result.tables?.count, 1)
        XCTAssertEqual(result.tables?[0].name, "0")
        XCTAssertEqual(result.tables?[0].columns?.count, 2)
        XCTAssertEqual(result.tables?[0].columns?[0].name, "_time")
        XCTAssertEqual(result.tables?[0].columns?[0].type, "datetime")
        XCTAssertEqual(result.tables?[0].events?.count, 1)
    }
    
    // MARK: - QueryStatus Tests
    
    func testQueryStatusDecoding() throws {
        let json = """
        {
            "elapsedTime": 123,
            "rowsExamined": 456,
            "rowsMatched": 78
        }
        """.data(using: .utf8)!
        
        let status = try JSONDecoder().decode(QueryStatus.self, from: json)
        
        XCTAssertEqual(status.elapsedTime, 123)
        XCTAssertEqual(status.rowsExamined, 456)
        XCTAssertEqual(status.rowsMatched, 78)
    }
    
    // MARK: - QueryTable Tests
    
    func testQueryTableDecodingWithNullColumnsAndEvents() throws {
        let json = """
        {
            "name": "empty-table"
        }
        """.data(using: .utf8)!
        
        let table = try JSONDecoder().decode(QueryTable.self, from: json)
        
        XCTAssertEqual(table.name, "empty-table")
        XCTAssertNil(table.columns)
        XCTAssertNil(table.events)
    }
    
    // MARK: - QueryColumn Tests
    
    func testQueryColumnDecoding() throws {
        let json = """
        {
            "name": "status_code",
            "type": "integer"
        }
        """.data(using: .utf8)!
        
        let column = try JSONDecoder().decode(QueryColumn.self, from: json)
        
        XCTAssertEqual(column.name, "status_code")
        XCTAssertEqual(column.type, "integer")
    }
    
    // MARK: - QueryFormat Tests
    
    func testQueryFormatRawValues() {
        XCTAssertEqual(QueryFormat.tabular.rawValue, "tabular")
        XCTAssertEqual(QueryFormat.legacy.rawValue, "legacy")
    }
    
    // MARK: - AxiomError Tests
    
    func testAxiomErrorAuthenticationFailed() {
        let error = AxiomError.authenticationFailed
        XCTAssertNotNil(error)
    }
    
    func testAxiomErrorRateLimited() {
        let error = AxiomError.rateLimited
        XCTAssertNotNil(error)
    }
    
    func testAxiomErrorHttpError() {
        let error = AxiomError.httpError(500)
        if case .httpError(let code) = error {
            XCTAssertEqual(code, 500)
        } else {
            XCTFail("Expected httpError")
        }
    }
    
    func testAxiomErrorConfigNotFound() {
        let error = AxiomError.configNotFound
        XCTAssertNotNil(error)
    }
    
    func testAxiomErrorInvalidConfig() {
        let error = AxiomError.invalidConfig("Test message")
        if case .invalidConfig(let message) = error {
            XCTAssertEqual(message, "Test message")
        } else {
            XCTFail("Expected invalidConfig error")
        }
    }
    
    // MARK: - AnyCodable Tests
    
    func testAnyCodableDecodeNull() throws {
        let json = "null".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        XCTAssertTrue(decoded.value is NSNull)
    }
    
    func testAnyCodableDecodeBool() throws {
        let jsonTrue = "true".data(using: .utf8)!
        let jsonFalse = "false".data(using: .utf8)!
        
        let decodedTrue = try JSONDecoder().decode(AnyCodable.self, from: jsonTrue)
        let decodedFalse = try JSONDecoder().decode(AnyCodable.self, from: jsonFalse)
        
        XCTAssertEqual(decodedTrue.value as? Bool, true)
        XCTAssertEqual(decodedFalse.value as? Bool, false)
    }
    
    func testAnyCodableDecodeInt() throws {
        let json = "42".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        XCTAssertEqual(decoded.value as? Int, 42)
    }
    
    func testAnyCodableDecodeDouble() throws {
        let json = "3.14159".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        let value = try XCTUnwrap(decoded.value as? Double)
        XCTAssertEqual(value, 3.14159, accuracy: 0.00001)
    }
    
    func testAnyCodableDecodeString() throws {
        let json = "\"hello world\"".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        XCTAssertEqual(decoded.value as? String, "hello world")
    }
    
    func testAnyCodableDecodeArray() throws {
        let json = "[1, 2, 3]".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        let array = decoded.value as? [Any]
        XCTAssertNotNil(array)
        XCTAssertEqual(array?.count, 3)
    }
    
    func testAnyCodableDecodeMixedArray() throws {
        let json = "[1, \"two\", true, null]".data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        let array = decoded.value as? [Any]
        XCTAssertNotNil(array)
        XCTAssertEqual(array?.count, 4)
        XCTAssertEqual(array?[0] as? Int, 1)
        XCTAssertEqual(array?[1] as? String, "two")
        XCTAssertEqual(array?[2] as? Bool, true)
        XCTAssertTrue(array?[3] is NSNull)
    }
    
    func testAnyCodableDecodeDictionary() throws {
        let json = """
        {"name": "test", "count": 42, "active": true}
        """.data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        let dict = decoded.value as? [String: Any]
        XCTAssertNotNil(dict)
        XCTAssertEqual(dict?["name"] as? String, "test")
        XCTAssertEqual(dict?["count"] as? Int, 42)
        XCTAssertEqual(dict?["active"] as? Bool, true)
    }
    
    func testAnyCodableDecodeNestedDictionary() throws {
        let json = """
        {"outer": {"inner": "value"}}
        """.data(using: .utf8)!
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: json)
        let dict = decoded.value as? [String: Any]
        XCTAssertNotNil(dict)
        let outer = dict?["outer"] as? [String: Any]
        XCTAssertEqual(outer?["inner"] as? String, "value")
    }
    
    func testAnyCodableEncodeNull() throws {
        let value = AnyCodable(NSNull())
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "null")
    }
    
    func testAnyCodableEncodeBool() throws {
        let value = AnyCodable(true)
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "true")
    }
    
    func testAnyCodableEncodeInt() throws {
        let value = AnyCodable(42)
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "42")
    }
    
    func testAnyCodableEncodeDouble() throws {
        let value = AnyCodable(3.5)
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "3.5")
    }
    
    func testAnyCodableEncodeString() throws {
        let value = AnyCodable("hello")
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "\"hello\"")
    }
    
    func testAnyCodableEncodeArray() throws {
        let value = AnyCodable([1, 2, 3])
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "[1,2,3]")
    }
    
    func testAnyCodableEncodeDictionary() throws {
        let value = AnyCodable(["key": "value"])
        let data = try JSONEncoder().encode(value)
        let string = String(data: data, encoding: .utf8)
        XCTAssertEqual(string, "{\"key\":\"value\"}")
    }
    
    func testAnyCodableRoundTrip() throws {
        let original: [String: Any] = [
            "string": "test",
            "int": 42,
            "double": 3.14,
            "bool": true,
            "null": NSNull(),
            "array": [1, 2, 3],
            "nested": ["key": "value"]
        ]
        
        let encoded = try JSONEncoder().encode(AnyCodable(original))
        let decoded = try JSONDecoder().decode(AnyCodable.self, from: encoded)
        let result = decoded.value as? [String: Any]
        
        XCTAssertNotNil(result)
        XCTAssertEqual(result?["string"] as? String, "test")
        XCTAssertEqual(result?["int"] as? Int, 42)
        XCTAssertEqual(result?["bool"] as? Bool, true)
        XCTAssertTrue(result?["null"] is NSNull)
    }
}
