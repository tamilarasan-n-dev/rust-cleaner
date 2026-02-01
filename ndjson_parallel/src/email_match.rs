use serde_json::Value;

pub fn matches_email(v: &Value) -> bool {
    if let Some(emails) = v.get("emails").and_then(|e| e.as_array()) {
        return emails.iter().any(|e| {
            e.get("address")
                .and_then(|a| a.as_str())
                .map(|addr| addr.ends_with("@google.com"))
                .unwrap_or(false)
        });
    }
    false
}
