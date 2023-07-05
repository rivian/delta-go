module github.com/rivian/delta-go

go 1.20

require (
	cirello.io/dynamolock v1.4.0
	github.com/aws/aws-sdk-go v1.44.200
	github.com/aws/aws-sdk-go-v2 v1.18.1
	github.com/aws/smithy-go v1.13.5
	github.com/go-redsync/redsync/v4 v4.8.1
	github.com/gofrs/flock v0.8.1
	github.com/google/uuid v1.3.0
	github.com/iancoleman/strcase v0.2.0
	github.com/redis/go-redis/v9 v9.0.2
	github.com/segmentio/parquet-go v0.0.0-20230427215636-d483faba23a5
	github.com/sirupsen/logrus v1.9.0
	github.com/stvp/tempredis v0.0.0-20181119212430-b82af8480203
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20200817004010-026bad9b25d0
	golang.org/x/exp v0.0.0-20230224173230-c95f2b4c22f2
)

require (
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.28 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.24 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/rivo/uniseg v0.1.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

require (
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.30.6
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gomodule/redigo v1.8.9 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/segmentio/encoding v0.3.5 // indirect
	golang.org/x/sys v0.5.0 // indirect
)

replace github.com/segmentio/parquet-go v0.0.0-20230427215636-d483faba23a5 => github.com/chelseajonesr/parquet-go v0.0.3
