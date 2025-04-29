// swift-tools-version:5.10
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let swiftSettings: [SwiftSetting] = [.enableExperimentalFeature("StrictConcurrency=complete")]

let package = Package(
    name: "swift-jobs",
    platforms: [.macOS(.v14), .iOS(.v17), .tvOS(.v17)],
    products: [
        .library(name: "Jobs", targets: ["Jobs"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.1.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0"..<"3.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.63.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.0.0"),
    ],
    targets: [
        .target(
            name: "Jobs",
            dependencies: [
                .product(name: "Collections", package: "swift-collections"),
                .product(name: "Tracing", package: "swift-distributed-tracing"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
            ],
            swiftSettings: swiftSettings
        ),
        // test targets
        .testTarget(
            name: "JobsTests",
            dependencies: [
                .byName(name: "Jobs"),
                .product(name: "Atomics", package: "swift-atomics"),
            ]
        ),
    ]
)
