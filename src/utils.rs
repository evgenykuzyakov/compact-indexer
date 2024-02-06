use crate::*;
use near_indexer::near_primitives::types::AccountId;
use serde::Deserialize;

pub fn extract_return_value_int(execution_status: ExecutionStatusView) -> Option<u128> {
    if let ExecutionStatusView::SuccessValue(value) = execution_status {
        let str_value = serde_json::from_slice::<String>(&value).ok()?;
        str_value.parse::<u128>().ok()
    } else {
        None
    }
}

#[derive(Deserialize)]
pub struct ArgsData {
    pub account_id: Option<AccountId>,
    pub args_new_account_id: Option<AccountId>,
    pub args_owner_id: Option<AccountId>,
    pub receiver_id: Option<AccountId>,
    pub sender_id: Option<AccountId>,
    pub token_id: Option<String>,
    pub nft_contract_id: Option<AccountId>,
    pub nft_token_id: Option<String>,
    pub amount: Option<String>,
    pub balance: Option<String>,
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
            if args_data
                .nft_token_id
                .as_ref()
                .map(|s| s.len())
                .unwrap_or(0)
                > MAX_TOKEN_LENGTH
            {
                args_data.nft_token_id = None;
            }
            Some(args_data)
        }
        _ => None,
    }
}
