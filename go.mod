module github.com/XGC-Team/xgc2-storage

go 1.25.0

require (
	github.com/XGC-Team/xgc2-xrpc/go v0.0.0-20261008201732-a26df3039fa7
	golang.org/x/sys v0.42.0
	google.golang.org/grpc v1.77.0
	google.golang.org/protobuf v1.36.10
	modernc.org/sqlite v1.46.2
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/net v0.46.1-0.20251013234738-63d1a5100f82 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
	modernc.org/libc v1.70.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

// Development only: the shared XRPC module comes from the sibling checkout until a
// release of it is pinned above. This code needs its plain limits API (xrpc-go commit
// 5ee3c17 or newer; NewDiagnostics lost its policy argument there). The pin above
// predates it. scripts/build-package.py refuses to package while a replace exists.
replace github.com/XGC-Team/xgc2-xrpc/go => ../xrpc-go/go
