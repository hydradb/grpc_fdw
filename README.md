# GRPC ForeignDataWrapper

A GRPC FDW for Postgres!

## Features

* Simple interface (`execute`, `insert`, `update`, `delete`)
* Implement Foreign Table Servers in any language which has GRPC support

## Example

```sql
CREATE FOREIGN DATA WRAPPER grpc_fdw_handler HANDLER grpc_fdw_handler NO VALIDATOR;
CREATE SERVER user_srv FOREIGN DATA WRAPPER grpc_fdw_handler OPTIONS (server_uri 'http://[::1]:50051');
CREATE FOREIGN TABLE users (
    id integer,
    name text,
    email text
) SERVER user_srv OPTIONS (
    table_option '1',
    table_option2 '2'
);
```

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
