import FileProvider
import UniformTypeIdentifiers

class FileProviderExtension: NSObject, NSFileProviderReplicatedExtension {
    let domain: NSFileProviderDomain
    let manager: NSFileProviderManager
    
    // In-memory item store (replace with persistent store for production)
    private var items: [NSFileProviderItemIdentifier: FileProviderItem] = [:]
    
    required init(domain: NSFileProviderDomain) {
        self.domain = domain
        self.manager = NSFileProviderManager(for: domain)!
        super.init()
        
        // Initialize root items
        setupRootItems()
    }
    
    private func setupRootItems() {
        // Root container
        let root = FileProviderItem(
            identifier: .rootContainer,
            parentIdentifier: .rootContainer,
            filename: "Axiom",
            contentType: .folder
        )
        items[.rootContainer] = root
        
        // Datasets folder
        let datasetsID = NSFileProviderItemIdentifier("datasets")
        let datasets = FileProviderItem(
            identifier: datasetsID,
            parentIdentifier: .rootContainer,
            filename: "datasets",
            contentType: .folder
        )
        items[datasetsID] = datasets
        
        // _queries folder (writable)
        let queriesID = NSFileProviderItemIdentifier("_queries")
        let queries = FileProviderItem(
            identifier: queriesID,
            parentIdentifier: .rootContainer,
            filename: "_queries",
            contentType: .folder,
            capabilities: [.allowsReading, .allowsWriting, .allowsAddingSubItems, .allowsContentEnumerating]
        )
        items[queriesID] = queries
    }
    
    func invalidate() {
        // Clean up
    }
    
    // MARK: - Item Access
    
    func item(for identifier: NSFileProviderItemIdentifier,
              request: NSFileProviderRequest,
              completionHandler: @escaping (NSFileProviderItem?, Error?) -> Void) -> Progress {
        
        if let item = items[identifier] {
            completionHandler(item, nil)
        } else {
            completionHandler(nil, NSFileProviderError(.noSuchItem))
        }
        
        return Progress()
    }
    
    // MARK: - Content Fetching
    
    func fetchContents(for itemIdentifier: NSFileProviderItemIdentifier,
                       version requestedVersion: NSFileProviderItemVersion?,
                       request: NSFileProviderRequest,
                       completionHandler: @escaping (URL?, NSFileProviderItem?, Error?) -> Void) -> Progress {
        
        guard let item = items[itemIdentifier] else {
            completionHandler(nil, nil, NSFileProviderError(.noSuchItem))
            return Progress()
        }
        
        // Create temporary file with content
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension(item.filename.components(separatedBy: ".").last ?? "txt")
        
        Task {
            do {
                // TODO: Fetch actual content from Axiom API
                let content = "# \(item.filename)\n\nContent would be fetched from Axiom API"
                try content.write(to: tempURL, atomically: true, encoding: .utf8)
                completionHandler(tempURL, item, nil)
            } catch {
                completionHandler(nil, nil, error)
            }
        }
        
        return Progress()
    }
    
    // MARK: - Enumeration
    
    func enumerator(for containerItemIdentifier: NSFileProviderItemIdentifier,
                    request: NSFileProviderRequest) throws -> NSFileProviderEnumerator {
        return FileProviderEnumerator(containerIdentifier: containerItemIdentifier, items: items)
    }
    
    // MARK: - Write Operations (for _queries)
    
    func createItem(basedOn itemTemplate: NSFileProviderItem,
                    fields: NSFileProviderItemFields,
                    contents url: URL?,
                    options: NSFileProviderCreateItemOptions = [],
                    request: NSFileProviderRequest,
                    completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        
        let newID = NSFileProviderItemIdentifier(UUID().uuidString)
        let newItem = FileProviderItem(
            identifier: newID,
            parentIdentifier: itemTemplate.parentItemIdentifier,
            filename: itemTemplate.filename,
            contentType: itemTemplate.contentType ?? .plainText,
            capabilities: [.allowsReading, .allowsWriting, .allowsDeleting, .allowsRenaming]
        )
        
        items[newID] = newItem
        
        // TODO: Upload to Axiom saved queries API
        
        completionHandler(newItem, [], false, nil)
        return Progress()
    }
    
    func modifyItem(_ item: NSFileProviderItem,
                    baseVersion version: NSFileProviderItemVersion,
                    changedFields: NSFileProviderItemFields,
                    contents newContents: URL?,
                    options: NSFileProviderModifyItemOptions = [],
                    request: NSFileProviderRequest,
                    completionHandler: @escaping (NSFileProviderItem?, NSFileProviderItemFields, Bool, Error?) -> Void) -> Progress {
        
        guard var existingItem = items[item.itemIdentifier] as? FileProviderItem else {
            completionHandler(nil, [], false, NSFileProviderError(.noSuchItem))
            return Progress()
        }
        
        // Update fields
        if changedFields.contains(.filename) {
            existingItem = FileProviderItem(
                identifier: existingItem.itemIdentifier,
                parentIdentifier: existingItem.parentItemIdentifier,
                filename: item.filename,
                contentType: existingItem.contentType ?? .plainText,
                capabilities: existingItem.capabilities
            )
        }
        
        items[item.itemIdentifier] = existingItem
        
        // TODO: Update in Axiom saved queries API
        
        completionHandler(existingItem, [], false, nil)
        return Progress()
    }
    
    func deleteItem(identifier: NSFileProviderItemIdentifier,
                    baseVersion version: NSFileProviderItemVersion,
                    options: NSFileProviderDeleteItemOptions = [],
                    request: NSFileProviderRequest,
                    completionHandler: @escaping (Error?) -> Void) -> Progress {
        
        items.removeValue(forKey: identifier)
        
        // TODO: Delete from Axiom saved queries API
        
        completionHandler(nil)
        return Progress()
    }
}
