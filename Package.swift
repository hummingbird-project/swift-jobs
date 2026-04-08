// swift-tools-version:6.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

var defaultSwiftSettings: [SwiftSetting] =
    [
        // https://github.com/apple/swift-evolution/blob/main/proposals/0335-existential-any.md
        .enableUpcomingFeature("ExistentialAny"),

        // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
        .enableUpcomingFeature("MemberImportVisibility"),

        // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0409-access-level-on-imports.md
        .enableUpcomingFeature("InternalImportsByDefault"),
    ]

#if compiler(>=6.2)
defaultSwiftSettings.append(contentsOf: [
    // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0461-async-function-isolation.md
    .enableUpcomingFeature("NonisolatedNonsendingByDefault")
])
#endif

let package = Package(
    name: "swift-jobs",
    platforms: [.macOS(.v15), .iOS(.v18), .tvOS(.v18)],
    products: [
        .library(name: "Jobs", targets: ["Jobs"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-distributed-tracing.git", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0"..<"3.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.63.0"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.0.0"),
        .package(url: "https://github.com/swift-extras/swift-extras-base64.git", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "Jobs",
            dependencies: [
                .product(name: "DequeModule", package: "swift-collections"),
                .product(name: "Tracing", package: "swift-distributed-tracing"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
                .product(name: "ExtrasBase64", package: "swift-extras-base64"),
            ],
            swiftSettings: defaultSwiftSettings
        ),
        // test targets
        .testTarget(
            name: "JobsTests",
            dependencies: [
                .byName(name: "Jobs"),
                .product(name: "MetricsTestKit", package: "swift-metrics"),
            ]
        ),
    ]
)
