import FileProvider

class FileProviderEnumerator: NSObject, NSFileProviderEnumerator {
    let containerIdentifier: NSFileProviderItemIdentifier
    let items: [NSFileProviderItemIdentifier: FileProviderItem]
    
    init(containerIdentifier: NSFileProviderItemIdentifier,
         items: [NSFileProviderItemIdentifier: FileProviderItem]) {
        self.containerIdentifier = containerIdentifier
        self.items = items
        super.init()
    }
    
    func invalidate() {
        // Clean up
    }
    
    func enumerateItems(for observer: NSFileProviderEnumerationObserver, startingAt page: NSFileProviderPage) {
        // Find all items whose parent is this container
        let children = items.values.filter { item in
            item.parentItemIdentifier == containerIdentifier && item.itemIdentifier != containerIdentifier
        }
        
        observer.didEnumerate(children)
        observer.finishEnumerating(upTo: nil)
    }
    
    func enumerateChanges(for observer: NSFileProviderChangeObserver, from anchor: NSFileProviderSyncAnchor) {
        // For now, report no changes
        observer.finishEnumeratingChanges(upTo: anchor, moreComing: false)
    }
    
    func currentSyncAnchor(completionHandler: @escaping (NSFileProviderSyncAnchor?) -> Void) {
        let anchor = NSFileProviderSyncAnchor(Date().description.data(using: .utf8)!)
        completionHandler(anchor)
    }
}
