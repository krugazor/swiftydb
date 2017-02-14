import PackageDescription

let package = Package(
    name: "SwiftyDB",
        dependencies: [
        .Package(url: "https://github.com/krugazor/tinysqlite.git", majorVersion: 0, minor: 4),
	]
)
