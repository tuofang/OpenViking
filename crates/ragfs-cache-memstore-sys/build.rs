use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=MEMSTORE_SDK_INCLUDE");
    println!("cargo:rerun-if-env-changed=MEMSTORE_SDK_LIB_DIR");
    println!("cargo:rerun-if-env-changed=MEMSTORE_SDK_LIB_NAME");

    if env::var_os("CARGO_FEATURE_NATIVE").is_none() {
        return;
    }

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        panic!("feature `native` is supported only on Linux");
    }

    let include_dir = required_dir("MEMSTORE_SDK_INCLUDE");
    let header = find_header(&include_dir).unwrap_or_else(|| {
        panic!(
            "MEMSTORE_SDK_INCLUDE must contain mms_c.h or mms/mms_c.h: {}",
            include_dir.display()
        )
    });
    let lib_dir = required_dir("MEMSTORE_SDK_LIB_DIR");
    let lib_name = env::var("MEMSTORE_SDK_LIB_NAME").unwrap_or_else(|_| "mms_client".into());

    println!("cargo:rerun-if-changed={}", header.display());
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib={lib_name}");
}

fn required_dir(name: &str) -> PathBuf {
    let path = PathBuf::from(env::var_os(name).unwrap_or_else(|| {
        panic!("{name} must point to an SDK directory when feature `native` is enabled")
    }));

    if !path.is_dir() {
        panic!(
            "{name} must point to an existing directory: {}",
            path.display()
        );
    }

    path
}

fn find_header(include_dir: &PathBuf) -> Option<PathBuf> {
    [include_dir.join("mms_c.h"), include_dir.join("mms/mms_c.h")]
        .into_iter()
        .find(|header| header.is_file())
}
