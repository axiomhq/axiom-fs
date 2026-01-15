import Foundation

actor ResponseCache {
    private var cache: [String: CacheEntry] = [:]
    private let ttl: TimeInterval
    
    init(ttl: TimeInterval = 60) {
        self.ttl = ttl
    }
    
    func get<T: Decodable>(_ key: String, as type: T.Type) -> T? {
        guard let entry = cache[key],
              Date().timeIntervalSince(entry.timestamp) < ttl else {
            cache.removeValue(forKey: key)
            return nil
        }
        return try? JSONDecoder().decode(type, from: entry.data)
    }
    
    func set(_ key: String, data: Data) {
        cache[key] = CacheEntry(data: data, timestamp: Date())
    }
    
    func invalidate(_ key: String) {
        cache.removeValue(forKey: key)
    }
    
    func invalidateAll() {
        cache.removeAll()
    }
}

private struct CacheEntry {
    let data: Data
    let timestamp: Date
}
