use super::ConfigError;
use crate::extension::WasmRuntime;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static FORCE_WASM_RUNTIME_INIT_FAILURE: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn wasm_runtime_for_database() -> Result<WasmRuntime, ConfigError> {
    #[cfg(test)]
    if FORCE_WASM_RUNTIME_INIT_FAILURE.with(Cell::get) {
        return Err(ConfigError::RuntimeInitialization(
            "forced wasm runtime init failure".to_owned(),
        ));
    }

    WasmRuntime::new().map_err(|error| ConfigError::RuntimeInitialization(error.to_string()))
}

#[cfg(test)]
pub(super) fn force_wasm_runtime_init_failure(enabled: bool) {
    FORCE_WASM_RUNTIME_INIT_FAILURE.with(|force_failure| force_failure.set(enabled));
}
