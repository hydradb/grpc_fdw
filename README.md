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

* Create handler

```sql
CREATE FOREIGN DATA WRAPPER grpc_fdw_handler HANDLER grpc_fdw_handler NO VALIDATOR;
CREATE SERVER user_srv FOREIGN DATA WRAPPER grpc_fdw_handler OPTIONS (server_uri 'http://[::1]:50051');
create foreign table hello_world (
    message text,
    from_server text,
    server_version integer
) server user_srv options (
    table_option '1',
    table_option2 '2'
);
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
