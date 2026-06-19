use serde_json::Value;

/// Evaluate a condition expression against the current workflow payload.
///
/// Format: `path.to.field <op> <value>`
///
/// Supported operators: `<`, `>`, `<=`, `>=`, `==`, `!=`
///
/// # Examples
///
/// ```ignore
/// assert!(evaluate("state.task_type == \"frontend\"", &payload));
/// assert!(!evaluate("state.coverage > 80", &payload));
/// ```
pub fn evaluate(condition: &str, payload: &Value) -> bool {
    let parts: Vec<&str> = condition.splitn(3, ' ').collect();
    if parts.len() != 3 {
        return true;
    }

    let path = parts[0];
    let op = parts[1];
    let expected_str = parts[2];

    let actual = resolve_path(payload, path);
    match (actual, op) {
        (Some(actual_val), "<" | ">" | "<=" | ">=") => {
            let actual_f = actual_val.as_f64().unwrap_or(f64::NAN);
            let expected_f: f64 = expected_str.parse().unwrap_or(f64::NAN);
            match op {
                "<" => actual_f < expected_f,
                ">" => actual_f > expected_f,
                "<=" => actual_f <= expected_f,
                ">=" => actual_f >= expected_f,
                _ => unreachable!(),
            }
        }
        (Some(actual_val), "==") | (Some(actual_val), "!=") => {
            let expected: Value = serde_json::from_str(expected_str)
                .unwrap_or(Value::String(expected_str.trim_matches('"').to_owned()));
            let eq = actual_val == &expected;
            if op == "==" { eq } else { !eq }
        }
        _ => true,
    }
}

/// Resolve a dot-separated path against a JSON value.
/// e.g. `resolve_path(payload, "state.test_coverage")` returns
/// `payload["state"]["test_coverage"]`.
fn resolve_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_numeric_less_than() {
        let payload = json!({"state": {"coverage": 75}});
        assert!(evaluate("state.coverage < 80", &payload));
        assert!(!evaluate("state.coverage < 70", &payload));
    }

    #[test]
    fn test_numeric_greater_than() {
        let payload = json!({"state": {"score": 95}});
        assert!(evaluate("state.score > 90", &payload));
        assert!(!evaluate("state.score > 99", &payload));
    }

    #[test]
    fn test_string_equality() {
        let payload = json!({"state": {"task_type": "frontend"}});
        assert!(evaluate(r#"state.task_type == "frontend""#, &payload));
        assert!(!evaluate(r#"state.task_type == "backend""#, &payload));
    }

    #[test]
    fn test_boolean_inequality() {
        let payload = json!({"state": {"has_errors": true}});
        assert!(evaluate("state.has_errors != false", &payload));
        assert!(!evaluate("state.has_errors != true", &payload));
    }

    #[test]
    fn test_missing_field_passes() {
        let payload = json!({"state": {}});
        assert!(evaluate("state.missing < 80", &payload));
        assert!(evaluate(r#"state.nonexistent == "value""#, &payload));
    }

    #[test]
    fn test_nested_path_resolution() {
        let payload = json!({"data": {"results": [{"name": "test"}], "meta": {"count": 42}}});
        assert!(evaluate("data.meta.count >= 40", &payload));
        let payload = json!({"data": {"results": [{"name": "test"}]}});
        assert!(evaluate(r#"data.results.0.name == "test""#, &payload));
    }
}
