pub fn is_match(key: &str, pattern: &str) -> bool {
    let key_bytes = key.as_bytes();
    let pattern_bytes = pattern.as_bytes();

    let mut key_idx = 0;
    let mut pattern_idx = 0;
    let mut star_idx = None;
    let mut match_idx = 0;

    while key_idx < key_bytes.len() {
        if pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'*' {
            // Found a star, save position and try to match rest
            star_idx = Some(pattern_idx);
            match_idx = key_idx;
            pattern_idx += 1;
        } else if pattern_idx < pattern_bytes.len()
            && pattern_bytes[pattern_idx] == key_bytes[key_idx]
        {
            // Characters match, advance both
            key_idx += 1;
            pattern_idx += 1;
        } else if let Some(star) = star_idx {
            // No match, but we have a star to backtrack to
            // Try matching one more character with the star
            pattern_idx = star + 1;
            match_idx += 1;
            key_idx = match_idx;
        } else {
            // No match and no star to backtrack to
            return false;
        }
    }

    // Handle remaining stars in pattern
    while pattern_idx < pattern_bytes.len() && pattern_bytes[pattern_idx] == b'*' {
        pattern_idx += 1;
    }

    // Match succeeds if we've consumed both strings
    pattern_idx == pattern_bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_match() {
        assert!(is_match("foo", "*"));
        assert!(is_match("foo", "f*"));
        assert!(is_match("foo", "f*o"));
        assert!(is_match("foo", "*o"));
        assert!(is_match("foo", "*o*"));
        assert!(!is_match("foo", "o"));
        assert!(!is_match("foo", "fo"));
        assert!(!is_match("foo", "oo"));
        assert!(!is_match("foo", "zoo"));
    }
}
