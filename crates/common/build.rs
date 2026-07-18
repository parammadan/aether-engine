// Compiles the gRPC contract in ../../proto into Rust at build time via `protoc`.
// The generated code lands in $OUT_DIR and is pulled in by `tonic::include_proto!`
// in src/lib.rs, so it is never checked into git — the .proto is the single source of truth.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../proto/aether.proto")?;
    // Re-run codegen only when the contract actually changes.
    println!("cargo:rerun-if-changed=../../proto/aether.proto");
    Ok(())
}
