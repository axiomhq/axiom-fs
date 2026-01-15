// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "AxiomKit",
    platforms: [.macOS(.v13)],
    products: [
        .library(name: "AxiomKit", targets: ["AxiomKit"]),
    ],
    targets: [
        .target(name: "AxiomKit"),
        .testTarget(name: "AxiomKitTests", dependencies: ["AxiomKit"]),
    ]
)
