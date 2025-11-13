//! Logging configuration and initialization.
//!
//! This module provides functionality for setting up structured, colored logging
//! with node-specific context.

use std::hash::{DefaultHasher, Hash, Hasher};

use ansi_term::Colour::{Blue, Cyan, Green, Purple, Red, Yellow};
use log::Level;

/// Initializes the logger with the specified verbosity level.
///
/// Sets up a logger that:
/// - Uses colored output for different log levels
/// - Includes timestamps
/// - Shows node context when available (via `distbench::get_node_context`)
///
/// # Arguments
///
/// * `verbose` - Verbosity level (0 = info, 1 = debug, 2+ = trace)
///
/// # Examples
///
/// ```ignore
/// use distbench::logging::init_logger;
///
/// init_logger(1); // Debug level logging
/// ```
pub fn init_logger(verbose: u8) {
    let level = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
        .format(|buf, record| {
            use std::io::Write;

            let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");

            let level_str = match record.level() {
                Level::Error => Red.paint("ERROR"),
                Level::Warn => Yellow.paint("WARN"),
                Level::Info => Green.paint("INFO"),
                Level::Debug => Blue.paint("DEBUG"),
                Level::Trace => Purple.paint("TRACE"),
            };

            let node_id = distbench::get_node_context()
                .map(|id| {
                    let mut hasher = DefaultHasher::new();
                    id.hash(&mut hasher);
                    let hash = hasher.finish();

                    let color = match hash % 6 {
                        0 => Cyan,
                        1 => Green,
                        2 => Yellow,
                        3 => Blue,
                        4 => Purple,
                        5 => Red,
                        _ => unreachable!(),
                    };
                    color.paint(format!("[{}] ", id)).to_string()
                })
                .unwrap_or_default();

            writeln!(buf, "{ts} {node_id}{level_str}: {}", record.args())
        })
        .init();
}
