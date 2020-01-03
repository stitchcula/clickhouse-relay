module clickhouse-relay

go 1.13

require (
	github.com/beeker1121/goque v2.1.0+incompatible
	github.com/golang/snappy v0.0.1
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/naoina/toml v0.1.1
	github.com/stitchcula/clickhouse-relay v0.0.0
	github.com/syndtr/goleveldb v1.0.0 // indirect
)

replace github.com/stitchcula/clickhouse-relay v0.0.0 => ./
