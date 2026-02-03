// Project:   dfe-receiver
// File:      build.rs
// Purpose:   Build script for proto compilation
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Vector proto
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/vector.proto"], &["proto"])?;

    // Tell cargo to rerun if proto changes
    println!("cargo:rerun-if-changed=proto/vector.proto");

    Ok(())
}
