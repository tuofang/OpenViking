use crate::frame::HEADER_LEN;
use ragfs::cache::{CacheError, CacheResult};

/// Connection, execution, and TLS settings for a MemStore provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemStoreConfig {
    pub net_connect_count: u16,
    pub net_group_count: u16,
    pub busy_polling: bool,
    pub sdk_concurrency: usize,
    pub operation_timeout_ms: u64,
    pub max_value_size_bytes: usize,
    pub tls_enabled: bool,
    pub certification_path: String,
    pub ca_cert_path: String,
    pub ca_crl_path: String,
    pub private_key_path: String,
    pub private_key_password_path: String,
    pub decrypter_lib_path: String,
    pub openssl_lib_dir: String,
}

impl Default for MemStoreConfig {
    fn default() -> Self {
        Self {
            net_connect_count: 16,
            net_group_count: 1,
            busy_polling: true,
            sdk_concurrency: 16,
            operation_timeout_ms: 5_000,
            max_value_size_bytes: 67_108_864,
            tls_enabled: false,
            certification_path: String::new(),
            ca_cert_path: String::new(),
            ca_crl_path: String::new(),
            private_key_path: String::new(),
            private_key_password_path: String::new(),
            decrypter_lib_path: String::new(),
            openssl_lib_dir: String::new(),
        }
    }
}

impl MemStoreConfig {
    pub(crate) fn validate(&self) -> CacheResult<()> {
        if self.net_connect_count == 0 {
            return invalid("MemStore net_connect_count must be greater than zero");
        }
        if self.net_group_count == 0 {
            return invalid("MemStore net_group_count must be greater than zero");
        }
        if self.sdk_concurrency == 0 || self.sdk_concurrency > u32::MAX as usize {
            return invalid("MemStore sdk_concurrency must be between 1 and u32::MAX");
        }
        if self.operation_timeout_ms == 0 {
            return invalid("MemStore operation_timeout_ms must be greater than zero");
        }
        if self.max_value_size_bytes == 0
            || self.max_value_size_bytes > u32::MAX as usize - HEADER_LEN
        {
            return invalid(
                "MemStore max_value_size_bytes must fit in a framed C unsigned integer value",
            );
        }

        let paths = [
            ("certification_path", &self.certification_path),
            ("ca_cert_path", &self.ca_cert_path),
            ("ca_crl_path", &self.ca_crl_path),
            ("private_key_path", &self.private_key_path),
            ("private_key_password_path", &self.private_key_password_path),
            ("decrypter_lib_path", &self.decrypter_lib_path),
            ("openssl_lib_dir", &self.openssl_lib_dir),
        ];
        for (name, path) in paths {
            if path.as_bytes().contains(&0) {
                return invalid(format!("MemStore {name} contains an embedded NUL byte"));
            }
            if path.len() >= libc::PATH_MAX as usize {
                return invalid(format!(
                    "MemStore {name} must be shorter than PATH_MAX bytes"
                ));
            }
            if self.tls_enabled && path.is_empty() {
                return invalid(format!("MemStore {name} is required when TLS is enabled"));
            }
        }

        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> CacheResult<T> {
    Err(CacheError::InvalidArgument(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragfs::cache::CacheError;

    fn valid_tls_config() -> MemStoreConfig {
        MemStoreConfig {
            tls_enabled: true,
            certification_path: "/tls/cert.pem".into(),
            ca_cert_path: "/tls/ca.pem".into(),
            ca_crl_path: "/tls/ca.crl".into(),
            private_key_path: "/tls/key.pem".into(),
            private_key_password_path: "/tls/password".into(),
            decrypter_lib_path: "/tls/decrypter.so".into(),
            openssl_lib_dir: "/tls/lib".into(),
            ..MemStoreConfig::default()
        }
    }

    #[test]
    fn defaults_match_memstore_sdk_settings() {
        assert_eq!(
            MemStoreConfig::default(),
            MemStoreConfig {
                net_connect_count: 16,
                net_group_count: 1,
                busy_polling: true,
                sdk_concurrency: 16,
                operation_timeout_ms: 5_000,
                max_value_size_bytes: 67_108_864,
                tls_enabled: false,
                certification_path: String::new(),
                ca_cert_path: String::new(),
                ca_crl_path: String::new(),
                private_key_path: String::new(),
                private_key_password_path: String::new(),
                decrypter_lib_path: String::new(),
                openssl_lib_dir: String::new(),
            }
        );
    }

    #[test]
    fn validation_rejects_zero_and_c_integer_overflow_values() {
        let mut config = MemStoreConfig::default();
        config.net_connect_count = 0;
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = MemStoreConfig::default();
        config.net_group_count = 0;
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = MemStoreConfig::default();
        config.sdk_concurrency = 0;
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = MemStoreConfig::default();
        config.operation_timeout_ms = 0;
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = MemStoreConfig::default();
        config.max_value_size_bytes = 0;
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        if usize::BITS > 32 {
            let mut config = MemStoreConfig::default();
            config.sdk_concurrency = u32::MAX as usize + 1;
            assert!(matches!(
                config.validate(),
                Err(CacheError::InvalidArgument(_))
            ));

            let mut config = MemStoreConfig::default();
            config.max_value_size_bytes = u32::MAX as usize - 8;
            assert!(matches!(
                config.validate(),
                Err(CacheError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn validation_rejects_invalid_tls_paths() {
        let mut config = valid_tls_config();
        config.private_key_path.clear();
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = valid_tls_config();
        config.ca_cert_path.push('\0');
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        let mut config = valid_tls_config();
        config.certification_path = "x".repeat(libc::PATH_MAX as usize);
        assert!(matches!(
            config.validate(),
            Err(CacheError::InvalidArgument(_))
        ));

        valid_tls_config().validate().unwrap();
    }
}
