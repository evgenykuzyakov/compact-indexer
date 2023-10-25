use crate::*;
use near_indexer::near_primitives::types::AccountId;
use serde::Deserialize;

pub fn extract_return_value_int(execution_status: ExecutionStatusView) -> Option<u128> {
    if let ExecutionStatusView::SuccessValue(value) = execution_status {
        String::from_utf8(value).ok()?.parse::<u128>().ok()
    }
    None
}

#[derive(Deserialize)]
pub struct U128(#[serde(with = "dec_format")] pub u128);

#[derive(Deserialize)]
pub struct ArgsData {
    pub account_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub token_id: Option<String>,
    pub amount: Option<U128>,
}

const MAX_TOKEN_LENGTH: usize = 64;

pub fn extract_args_data(action: &ActionView) -> Option<ArgsData> {
    match action {
        ActionView::FunctionCall { args, .. } => {
            let mut args_data: ArgsData = serde_json::from_slice(&args).ok()?;
            // If token length is larger than 64 bytes, we remove it.
            if args_data.token_id.as_ref().map(|s| s.len()).unwrap_or(0) > MAX_TOKEN_LENGTH {
                args_data.token_id = None;
            }
            Some(args_data)
        }
        _ => None,
    }
}
