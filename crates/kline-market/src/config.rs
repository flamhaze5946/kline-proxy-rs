use serde::Deserialize;
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub metadata_refresh_seconds: u64,
    pub funding: FundingConfig,
    pub statistics: StatisticsConfig,
    pub cms_url: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            metadata_refresh_seconds: 300,
            funding: FundingConfig::default(),
            statistics: StatisticsConfig::default(),
            cms_url: "https://www.binance.com".into(),
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FundingConfig {
    pub enabled: bool,
    pub publication_grace_ms: u64,
    pub vision_enabled: bool,
    pub vision_url: String,
    pub vision_days: u32,
    pub vision_workers: usize,
}
impl Default for FundingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            publication_grace_ms: 50,
            vision_enabled: true,
            vision_url: "https://data.binance.vision".into(),
            vision_days: 30,
            vision_workers: 16,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StatisticsConfig {
    pub enabled: bool,
    pub atr_symbol: String,
    pub atr_period: usize,
    pub start_date: String,
    pub days: usize,
    pub volume_days: usize,
    pub volume_rank: usize,
    pub altcoin_url: String,
    pub timezone_offset_minutes: Option<i32>,
}
impl Default for StatisticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            atr_symbol: "BTCUSDT".into(),
            atr_period: 48,
            start_date: "2021-01-01".into(),
            days: 30,
            volume_days: 7,
            volume_rank: 20,
            altcoin_url: "https://www.blockchaincenter.net/en/altcoin-season-index".into(),
            timezone_offset_minutes: None,
        }
    }
}
