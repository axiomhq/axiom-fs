import FileProvider
import UniformTypeIdentifiers

class FileProviderItem: NSObject, NSFileProviderItem {
    let itemIdentifier: NSFileProviderItemIdentifier
    let parentItemIdentifier: NSFileProviderItemIdentifier
    let filename: String
    let capabilities: NSFileProviderItemCapabilities
    
    private let _contentType: UTType
    private let _creationDate: Date
    private let _modificationDate: Date
    private let _size: Int64
    
    init(
        identifier: NSFileProviderItemIdentifier,
        parentIdentifier: NSFileProviderItemIdentifier,
        filename: String,
        contentType: UTType,
        capabilities: NSFileProviderItemCapabilities = [.allowsReading, .allowsContentEnumerating],
        size: Int64 = 0,
        creationDate: Date = Date(),
        modificationDate: Date = Date()
    ) {
        self.itemIdentifier = identifier
        self.parentItemIdentifier = parentIdentifier
        self.filename = filename
        self._contentType = contentType
        self.capabilities = capabilities
        self._size = size
        self._creationDate = creationDate
        self._modificationDate = modificationDate
        super.init()
    }
    
    var contentType: UTType {
        return _contentType
    }
    
    var documentSize: NSNumber? {
        return NSNumber(value: _size)
    }
    
    var creationDate: Date? {
        return _creationDate
    }
    
    var contentModificationDate: Date? {
        return _modificationDate
    }
    
    var itemVersion: NSFileProviderItemVersion {
        let contentVersion = _modificationDate.timeIntervalSince1970.description.data(using: .utf8) ?? Data()
        let metadataVersion = _modificationDate.timeIntervalSince1970.description.data(using: .utf8) ?? Data()
        return NSFileProviderItemVersion(contentVersion: contentVersion, metadataVersion: metadataVersion)
    }
}

// Convenience initializers for common item types
extension FileProviderItem {
    static func dataset(name: String) -> FileProviderItem {
        FileProviderItem(
            identifier: NSFileProviderItemIdentifier("dataset:\(name)"),
            parentIdentifier: NSFileProviderItemIdentifier("datasets"),
            filename: name,
            contentType: .folder
        )
    }
    
    static func schemaFile(for dataset: String) -> FileProviderItem {
        FileProviderItem(
            identifier: NSFileProviderItemIdentifier("dataset:\(dataset):schema"),
            parentIdentifier: NSFileProviderItemIdentifier("dataset:\(dataset)"),
            filename: "schema.json",
            contentType: .json
        )
    }
    
    static func queryFile(name: String, in dataset: String, path: String) -> FileProviderItem {
        FileProviderItem(
            identifier: NSFileProviderItemIdentifier("query:\(dataset):\(path)"),
            parentIdentifier: NSFileProviderItemIdentifier("dataset:\(dataset)"),
            filename: name,
            contentType: .json
        )
    }
}
