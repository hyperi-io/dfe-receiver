// Project:   dfe-receiver
// File:      build.rs
// Purpose:   Build script for proto compilation (Vector + OTLP + Prometheus)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile Vector proto definitions (event.proto + vector.proto)
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/vector.proto", "proto/event.proto"], &["proto"])?;

    // Compile OTLP proto definitions (logs, metrics, traces services)
    // Vendored from https://github.com/open-telemetry/opentelemetry-proto v1.5.0
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &[
                "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
                "proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
            ],
            &["proto"],
        )?;

    // Compile Prometheus Remote Write proto (v1)
    // Vendored from https://github.com/prometheus/prometheus prompb/
    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(&["proto/prometheus/remote.proto"], &["proto"])?;

    // Tell cargo to rerun if protos change
    println!("cargo:rerun-if-changed=proto/vector.proto");
    println!("cargo:rerun-if-changed=proto/event.proto");
    println!("cargo:rerun-if-changed=proto/opentelemetry/");
    println!("cargo:rerun-if-changed=proto/prometheus/");

    Ok(())
}
