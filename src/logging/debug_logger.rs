//! Debug logger for development and diagnostics
//!
//! Provides async logging that doesn't block the main thread.

use tokio::sync::mpsc;

#[derive(Clone)]
pub struct DebugLogger {
    tx: mpsc::UnboundedSender<DebugEvent>,
    debug_enabled: bool,
}

enum DebugEvent {
    Info(String),
    Warn(String),
    Error(String),
}

impl DebugLogger {
    pub fn new(debug_enabled: bool) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    DebugEvent::Info(msg) => println!("{}", msg),
                    DebugEvent::Warn(msg) => eprintln!("WARN: {}", msg),
                    DebugEvent::Error(msg) => eprintln!("ERROR: {}", msg),
                }
            }
        });
        Self { tx, debug_enabled }
    }

    pub fn info<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        if !self.debug_enabled {
            return;
        }
        if let Err(err) = self.tx.send(DebugEvent::Info(msg())) {
            eprintln!("FATAL: Debug logger channel closed: {}", err);
        }
    }

    pub fn warn<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        if let Err(err) = self.tx.send(DebugEvent::Warn(msg())) {
            eprintln!("FATAL: Debug logger channel closed: {}", err);
        }
    }

    pub fn error<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        if let Err(err) = self.tx.send(DebugEvent::Error(msg())) {
            eprintln!("FATAL: Debug logger channel closed: {}", err);
        }
    }

    pub fn latency<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        if let Err(err) = self.tx.send(DebugEvent::Warn(msg())) {
            eprintln!("FATAL: Debug logger channel closed: {}", err);
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.debug_enabled
    }
}
