# GRPC ForeignDataWrapper

A GRPC FDW for Postgres!

## Developing

* Start echoserver

```
cd fdw_server && cargo run --bin fdw-server
```

## Release
```
cargo pgx package

cargo deb
```
