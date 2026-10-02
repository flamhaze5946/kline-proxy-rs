//! Lifecycle adapters. HTTP/bulk and domain state never depend on network/disk recovery.
pub mod config;
pub mod directory;
pub mod legacy;
pub mod lifecycle;
pub mod listener;
pub mod recovery;
pub mod rest;
pub mod storage;
pub mod tickers;
