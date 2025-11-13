//! Procedural macros for the distributed algorithms framework.
//!
//! This crate provides attribute macros that simplify writing distributed algorithms:
//!
//! - `#[distbench::state]` - Transforms an algorithm state struct into a complete
//!   algorithm implementation with configuration support
//! - `#[distbench::handlers]` - Generates message handler dispatch logic from
//!   handler methods
//! - `#[distbench::message]` - Derives common traits for message types

mod config_parsing;
mod handler_parsing;
mod handlers_macro;
mod message_macro;
mod peer_generation;
mod state_macro;

use proc_macro::TokenStream;

/// Attribute macro for defining algorithm state.
///
/// This macro transforms a struct into a complete algorithm state with:
/// - Configuration field extraction via `#[distbench::config]`
/// - Generated Config struct
/// - Automatic peer management
/// - Self-termination support
///
/// # Example
///
/// ```ignore
/// #[distbench::state]
/// pub struct MyAlgorithm<T: Transport> {
///     #[distbench::config]
///     required_field: u32,
///
///     #[distbench::config(default = 10)]
///     optional_field: u32,
/// }
/// ```
#[proc_macro_attribute]
pub fn state(_attr: TokenStream, item: TokenStream) -> TokenStream {
    state_macro::algorithm_state_impl(item)
}

/// Attribute macro for defining message handlers.
///
/// This macro generates the `AlgorithmHandler` trait implementation and
/// corresponding peer methods from handler function signatures.
///
/// # Example
///
/// ```ignore
/// #[distbench::handlers]
/// impl<T: Transport> MyAlgorithm<T> {
///     async fn handle_message(&self, src: PeerId, msg: &MyMessage) {
///         // Handler implementation
///     }
///
///     async fn handle_request(&self, src: PeerId, req: &MyRequest) -> MyResponse {
///         // Handler that returns a response
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn handlers(_attr: TokenStream, item: TokenStream) -> TokenStream {
    handlers_macro::algorithm_handlers_impl(item)
}

/// Attribute macro for defining message types.
///
/// Automatically derives `Serialize`, `Deserialize`, `Clone`, and `Debug` for the type.
///
/// # Example
///
/// ```ignore
/// #[distbench::message]
/// pub struct MyMessage {
///     payload: String,
/// }
/// ```
#[proc_macro_attribute]
pub fn message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    message_macro::message_impl(item)
}
