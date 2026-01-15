import Foundation

public struct Dataset: Codable, Identifiable, Sendable {
    public let id: String
    public let name: String
    public let description: String?
    public let created: Date?
    
    enum CodingKeys: String, CodingKey {
        case id, name, description, created
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.name = try container.decode(String.self, forKey: .name)
        self.id = self.name // Use name as ID since Axiom uses name as identifier
        self.description = try container.decodeIfPresent(String.self, forKey: .description)
        self.created = try container.decodeIfPresent(Date.self, forKey: .created)
    }
}

public struct QueryResult: Codable, Sendable {
    public let status: QueryStatus
    public let tables: [QueryTable]?
    
    enum CodingKeys: String, CodingKey {
        case status, tables
    }
}

public struct QueryStatus: Codable, Sendable {
    public let elapsedTime: Int
    public let rowsExamined: Int
    public let rowsMatched: Int
}

public struct QueryTable: Codable, Sendable {
    public let name: String
    public let columns: [QueryColumn]?
    public let events: [[String: AnyCodable]]?
}

public struct QueryColumn: Codable, Sendable {
    public let name: String
    public let type: String
}

public enum QueryFormat: String, Sendable {
    case tabular
    case legacy
}

public enum AxiomError: Error, Sendable {
    case authenticationFailed
    case rateLimited
    case httpError(Int)
    case configNotFound
    case invalidConfig(String)
}

// MARK: - AnyCodable for dynamic JSON values

public struct AnyCodable: Codable, Sendable {
    public let value: Any
    
    public init(_ value: Any) {
        self.value = value
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        
        if container.decodeNil() {
            self.value = NSNull()
        } else if let bool = try? container.decode(Bool.self) {
            self.value = bool
        } else if let int = try? container.decode(Int.self) {
            self.value = int
        } else if let double = try? container.decode(Double.self) {
            self.value = double
        } else if let string = try? container.decode(String.self) {
            self.value = string
        } else if let array = try? container.decode([AnyCodable].self) {
            self.value = array.map { $0.value }
        } else if let dictionary = try? container.decode([String: AnyCodable].self) {
            self.value = dictionary.mapValues { $0.value }
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Cannot decode value")
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        
        switch value {
        case is NSNull:
            try container.encodeNil()
        case let bool as Bool:
            try container.encode(bool)
        case let int as Int:
            try container.encode(int)
        case let double as Double:
            try container.encode(double)
        case let string as String:
            try container.encode(string)
        case let array as [Any]:
            try container.encode(array.map { AnyCodable($0) })
        case let dictionary as [String: Any]:
            try container.encode(dictionary.mapValues { AnyCodable($0) })
        default:
            throw EncodingError.invalidValue(value, EncodingError.Context(codingPath: container.codingPath, debugDescription: "Cannot encode value"))
        }
    }
}
