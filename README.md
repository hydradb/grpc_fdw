# GRPC ForeignDataWrapper

A GRPC FDW for Postgres!

## Developing
```
cargo update
cargp pgx run pg13
```

* Start echoserver

```
cd fdw_server && cargo run --bin fdw-server
```

## Release
```
cargo pgx package

cargo deb
```

## inspect
```
dpkg -c ./target/debian/grpc_fdw_0.0.0_amd64.deb
```
