// Project:   dfe-receiver
// File:      build.rs
// Purpose:   Build script for proto compilation
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Vector proto definitions (event.proto + vector.proto)
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &["proto/vector.proto", "proto/event.proto"],
            &["proto"],
        )?;

    // Tell cargo to rerun if protos change
    println!("cargo:rerun-if-changed=proto/vector.proto");
    println!("cargo:rerun-if-changed=proto/event.proto");

    Ok(())
}
