//! 服务端或驱动返回的错误文本在打印前去掉令牌形态的长串，并截断。

const MAX_SHOWN: usize = 240;
const TOKEN_RUN: usize = 24;

fn token_char(char: char) -> bool {
    char.is_ascii_alphanumeric() || matches!(char, '+' | '/' | '=' | '_' | '.' | '~' | '-')
}

pub fn redact(text: &str) -> String {
    let mut out = String::new();
    let mut run = String::new();
    let flush = |run: &mut String, out: &mut String| {
        if run.chars().count() >= TOKEN_RUN {
            out.push_str("[redacted]");
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for char in text.chars() {
        if token_char(char) {
            run.push(char);
        } else {
            flush(&mut run, &mut out);
            out.push(if char.is_control() { '?' } else { char });
        }
    }
    flush(&mut run, &mut out);
    if out.chars().count() > MAX_SHOWN {
        out = out.chars().take(MAX_SHOWN).collect::<String>() + "...";
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_token_runs_are_removed() {
        let text =
            "invalid token eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiIxMDAwMSJ9.c2lnbmF0dXJl rejected\n";
        let shown = redact(text);
        assert_eq!(shown, "invalid token [redacted] rejected?");
        assert_eq!(
            redact("PreSend rejected: SOCIAL_NOT_FRIEND"),
            "PreSend rejected: SOCIAL_NOT_FRIEND"
        );
        assert!(redact(&"x ".repeat(400)).ends_with("..."));
    }
}
