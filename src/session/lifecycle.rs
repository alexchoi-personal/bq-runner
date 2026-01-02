use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub idle_timeout: Duration,
    pub max_sessions: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30 * 60),
            max_sessions: 100,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_config_default() {
        let config = SessionConfig::default();
        assert_eq!(config.idle_timeout, Duration::from_secs(30 * 60));
        assert_eq!(config.max_sessions, 100);
    }

    #[test]
    fn test_session_config_custom() {
        let config = SessionConfig {
            idle_timeout: Duration::from_secs(60),
            max_sessions: 50,
        };
        assert_eq!(config.idle_timeout, Duration::from_secs(60));
        assert_eq!(config.max_sessions, 50);
    }

    #[test]
    fn test_session_config_clone() {
        let config = SessionConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.idle_timeout, config.idle_timeout);
        assert_eq!(cloned.max_sessions, config.max_sessions);
    }

    #[test]
    fn test_session_config_debug() {
        let config = SessionConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("SessionConfig"));
        assert!(debug_str.contains("idle_timeout"));
        assert!(debug_str.contains("max_sessions"));
    }
}
