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
    // UTM
    #[serde(rename = "_utm_source")]
    pub utm_source: Option<String>,
    #[serde(rename = "_utm_medium")]
    pub utm_medium: Option<String>,
    #[serde(rename = "_utm_campaign")]
    pub utm_campaign: Option<String>,
    #[serde(rename = "_utm_term")]
    pub utm_term: Option<String>,
    #[serde(rename = "_utm_content")]
    pub utm_content: Option<String>,
}

const MAX_TOKEN_LENGTH: usize = 64;
const MAX_TOKEN_IDS_LENGTH: usize = 4;

fn limit_length(s: &mut Option<String>) {
    if s.as_ref().map(|s| s.len()).unwrap_or(0) > MAX_TOKEN_LENGTH {
        *s = None;
    }
}

pub fn extract_args_data(action: &ActionView) -> Option<ArgsData> {
    match action {
        ActionView::FunctionCall { args, .. } => {
            let mut args_data: ArgsData = serde_json::from_slice(&args).ok()?;
            // If token length is larger than 64 bytes, we remove it.
            limit_length(&mut args_data.token_id);
            limit_length(&mut args_data.nft_token_id);
            limit_length(&mut args_data.utm_source);
            limit_length(&mut args_data.utm_medium);
            limit_length(&mut args_data.utm_campaign);
            limit_length(&mut args_data.utm_term);
            limit_length(&mut args_data.utm_content);
            Some(args_data)
        }
        _ => None,
    }
}

#[derive(Deserialize)]
pub struct EventData {
    pub account_id: Option<AccountId>,
    pub owner_id: Option<AccountId>,
    pub old_owner_id: Option<AccountId>,
    pub new_owner_id: Option<AccountId>,
    pub liquidation_account_id: Option<AccountId>,
    pub authorized_id: Option<AccountId>,
    pub token_ids: Option<Vec<String>>,
    pub token_id: Option<String>,
    pub position: Option<String>,
    pub amount: Option<String>,
}

#[derive(Deserialize)]
pub struct Event {
    pub version: Option<String>,
    pub standard: Option<String>,
    pub event: Option<String>,
    pub data: Option<Vec<EventData>>,
}

pub fn parse_event(event: &str) -> Option<Event> {
    let mut event: Event = serde_json::from_str(&event).ok()?;
    limit_length(&mut event.version);
    limit_length(&mut event.standard);
    limit_length(&mut event.event);
    if let Some(data) = event.data.as_mut().and_then(|data| data.get_mut(0)) {
        if let Some(token_ids) = data.token_ids.as_mut() {
            token_ids.retain(|s| s.len() <= MAX_TOKEN_LENGTH);
            if token_ids.len() > MAX_TOKEN_IDS_LENGTH {
                token_ids.resize(MAX_TOKEN_IDS_LENGTH, "".to_string());
            }
        }
        limit_length(&mut data.token_id);
    }
    Some(event)
}
