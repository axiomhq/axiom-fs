import Foundation
import AppKit

@MainActor
class NFSProcessManager: ObservableObject {
    enum State: Equatable {
        case stopped
        case starting
        case running(pid: Int32)
        case error(String)

        var isRunning: Bool {
            if case .running = self { return true }
            return false
        }
    }

    @Published private(set) var state: State = .stopped
    @Published private(set) var serverOutput: String = ""

    private var process: Process?
    private var outputPipe: Pipe?
    private var errorPipe: Pipe?
    private var terminationObserver: NSObjectProtocol?

    init() {
        // Clean up on app termination
        terminationObserver = NotificationCenter.default.addObserver(
            forName: NSApplication.willTerminateNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.cleanupSync()
        }
    }

    deinit {
        if let observer = terminationObserver {
            NotificationCenter.default.removeObserver(observer)
        }
    }

    private func cleanupSync() {
        if let proc = process, proc.isRunning {
            proc.terminate()
        }
        // Sync unmount attempt
        let umount = Process()
        umount.executableURL = URL(fileURLWithPath: "/sbin/umount")
        umount.arguments = [mountPoint]
        try? umount.run()
        umount.waitUntilExit()
    }

    private var mountPoint: String {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("Axiom").path
    }
    private let nfsHost = "127.0.0.1"
    private let nfsPort = 12049

    var binaryPath: String? {
        // Check bundled binary first
        if let bundled = Bundle.main.path(forAuxiliaryExecutable: "axiom-fs") {
            return bundled
        }

        // Check common install locations
        let searchPaths = [
            "/usr/local/bin/axiom-fs",
            "/opt/homebrew/bin/axiom-fs",
            FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent("go/bin/axiom-fs").path,
            FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".local/bin/axiom-fs").path
        ]

        for path in searchPaths where FileManager.default.isExecutableFile(atPath: path) {
            return path
        }

        return nil
    }

    func start() async throws {
        guard state == .stopped || state.isError else { return }

        guard let binary = binaryPath else {
            state = .error("axiom-fs binary not found")
            throw NFSError.binaryNotFound
        }

        // Kill any existing axiom-fs processes that might be holding the port
        await killExistingProcesses()

        state = .starting
        serverOutput = ""

        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: binary)
        proc.arguments = ["-listen", "127.0.0.1:\(nfsPort)"]

        // Inherit environment for AXIOM_TOKEN etc.
        var env = ProcessInfo.processInfo.environment
        env["PATH"] = "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin"
        proc.environment = env

        // Capture stdout
        let outPipe = Pipe()
        proc.standardOutput = outPipe
        self.outputPipe = outPipe

        // Capture stderr
        let errPipe = Pipe()
        proc.standardError = errPipe
        self.errorPipe = errPipe

        outPipe.fileHandleForReading.readabilityHandler = { @Sendable [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let str = String(data: data, encoding: .utf8) else { return }
            Task { @MainActor [weak self] in
                self?.serverOutput += str
            }
        }

        errPipe.fileHandleForReading.readabilityHandler = { @Sendable [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty, let str = String(data: data, encoding: .utf8) else { return }
            Task { @MainActor [weak self] in
                self?.serverOutput += str
            }
        }

        proc.terminationHandler = { @Sendable [weak self] proc in
            let exitCode = proc.terminationStatus
            Task { @MainActor [weak self] in
                self?.handleTermination(exitCode: exitCode)
            }
        }

        do {
            try proc.run()
            self.process = proc

            // Wait briefly for server to start
            try await Task.sleep(nanoseconds: 500_000_000) // 0.5s

            if proc.isRunning {
                state = .running(pid: proc.processIdentifier)

                // Auto-mount
                try await mount()
            } else {
                state = .error("Server exited immediately")
            }
        } catch {
            state = .error(error.localizedDescription)
            throw error
        }
    }

    func stop() async {
        // Unmount first
        await unmount()

        guard let proc = process, proc.isRunning else {
            state = .stopped
            return
        }

        proc.terminate()

        // Give it a moment to exit gracefully
        try? await Task.sleep(nanoseconds: 500_000_000)

        if proc.isRunning {
            proc.interrupt()
        }

        process = nil
        outputPipe?.fileHandleForReading.readabilityHandler = nil
        errorPipe?.fileHandleForReading.readabilityHandler = nil
        outputPipe = nil
        errorPipe = nil
        state = .stopped
    }

    private func handleTermination(exitCode: Int32) {
        outputPipe?.fileHandleForReading.readabilityHandler = nil
        errorPipe?.fileHandleForReading.readabilityHandler = nil

        if exitCode == 0 || exitCode == 15 || exitCode == 2 { // Normal, SIGTERM, SIGINT
            state = .stopped
        } else {
            state = .error("Server exited with code \(exitCode)")
        }
        process = nil
    }

    private func killExistingProcesses() async {
        let task = Process()
        task.executableURL = URL(fileURLWithPath: "/usr/bin/pkill")
        task.arguments = ["-f", "axiom-fs.*-listen"]
        task.standardOutput = FileHandle.nullDevice
        task.standardError = FileHandle.nullDevice

        try? task.run()
        task.waitUntilExit()

        // Brief pause to let the port be released
        try? await Task.sleep(nanoseconds: 100_000_000) // 0.1s
    }

    private func mount() async throws {
        // Create mount point if needed
        let fileManager = FileManager.default
        if !fileManager.fileExists(atPath: mountPoint) {
            try fileManager.createDirectory(atPath: mountPoint, withIntermediateDirectories: true)
        }

        // Mount NFS using mount_nfs directly with noresvport (doesn't need privileged source port)
        let mountProc = Process()
        mountProc.executableURL = URL(fileURLWithPath: "/sbin/mount_nfs")
        mountProc.arguments = [
            "-o", "vers=3,tcp,port=\(nfsPort),mountport=\(nfsPort),noresvport,nolocks,locallocks",
            "\(nfsHost):/",
            mountPoint
        ]

        let pipe = Pipe()
        mountProc.standardError = pipe

        try mountProc.run()
        mountProc.waitUntilExit()

        if mountProc.terminationStatus != 0 {
            let errData = pipe.fileHandleForReading.readDataToEndOfFile()
            let errStr = String(data: errData, encoding: .utf8) ?? "Unknown error"
            throw NFSError.mountFailed(errStr)
        }
    }

    private func unmount() async {
        let umountProc = Process()
        umountProc.executableURL = URL(fileURLWithPath: "/sbin/umount")
        umountProc.arguments = [mountPoint]

        try? umountProc.run()
        umountProc.waitUntilExit()
    }

    func openInFinder() {
        guard state.isRunning else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: mountPoint))
    }

    var isError: Bool {
        if case .error = state { return true }
        return false
    }
}

extension NFSProcessManager.State {
    var isError: Bool {
        if case .error = self { return true }
        return false
    }
}

enum NFSError: LocalizedError {
    case binaryNotFound
    case mountFailed(String)

    var errorDescription: String? {
        switch self {
        case .binaryNotFound:
            return "axiom-fs binary not found. Install with: go install github.com/axiomhq/axiom-fs/cmd/axiom-fs@latest"
        case .mountFailed(let msg):
            return "Mount failed: \(msg)"
        }
    }
}
