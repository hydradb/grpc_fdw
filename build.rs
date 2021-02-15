fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::configure()
    //     .out_dir("./")
    //     .compile(&["proto/pg_fdw.proto"], &["proto/"])?;
    tonic_build::compile_protos("proto/pg_fdw.proto")?;
    Ok(())
}
