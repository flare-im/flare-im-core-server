//! 推送文案的清理与截断。
//!
//! 标题正文有一部分来自发送端（`offline_push_info`、消息里的文字）。发送端可能是恶意客户端，
//! 它写的字会出现在别人的锁屏上，所以进通知之前一律过这一道：
//!
//! - 去掉控制字符：换行之类能把一条通知排成几行，伪装出第二条通知的样子；
//! - 去掉双向文字的覆盖与隔离字符：它们能让一段文字倒着显示，把「文件.exe」排成别的样子；
//!   零宽空格、BOM 这类看不见的格式字符与韩文填充符也一并去掉，它们能拼出「看着是空白」的名字；
//! - 连续的空白（含换行、制表、全角空格）收成一个空格，去掉首尾空白；
//! - 按字素簇（用户眼里的一个字）计数截断：标题最多 64 个，正文最多 180 个，超出时末尾换成
//!   「…」，省略号算在上限里。不按字节或码点截，否则 emoji、国旗、带变音符的字母会被截成半个；
//! - 另有字节上限（标题 256、正文 1024，同样含省略号）：一个字素簇可以由很多码点组成
//!   （叠满变音符的「Zalgo」文字、长的 emoji 序列），按个数不长，按字节能把 APNs 4 KiB 的载荷
//!   撑破。单个字素簇超过 64 字节时只留它的第一个字符，叠出来的变音符不会盖住别的通知。
//!
//! 清理之后为空的文字当作没有，由调用方换成模板文案。
//!
//! 厂商自己的字段另有更短的字数上限（个推转给小米、OPPO、vivo 的标题正文），由
//! [`fit_utf16`] 在发给那一家之前再截一次。

use unicode_segmentation::UnicodeSegmentation;

/// 标题最多多少个字素簇（含省略号）。
pub const TITLE_MAX_CHARS: usize = 64;
/// 正文最多多少个字素簇（含省略号）。
pub const BODY_MAX_CHARS: usize = 180;
/// 标题最多多少字节（UTF-8，含省略号）。正常文字到不了：64 个汉字是 192 字节。
const TITLE_MAX_BYTES: usize = 256;
/// 正文最多多少字节（UTF-8，含省略号）。180 个汉字是 540 字节。
const BODY_MAX_BYTES: usize = 1024;
/// 单个字素簇的字节上限。最长的标准 emoji 序列（带肤色的情侣、苏格兰旗）约 35 字节。
const CLUSTER_MAX_BYTES: usize = 64;
const ELLIPSIS: &str = "…";

/// 清理并截断一段要当标题的文字；清理后为空返回 `None`。
pub fn clean_title(raw: &str) -> Option<String> {
    clean(raw).map(|text| fit_title(&text))
}

/// 清理并截断一段要当正文的文字；清理后为空返回 `None`。
pub fn clean_body(raw: &str) -> Option<String> {
    clean(raw).map(|text| fit_body(&text))
}

/// 把已经清理过的文字（可能拼上了模板）截到标题的上限。
pub fn fit_title(text: &str) -> String {
    truncate(text, TITLE_MAX_CHARS, TITLE_MAX_BYTES)
}

/// 把已经清理过的文字（可能拼上了模板）截到正文的上限。
pub fn fit_body(text: &str) -> String {
    truncate(text, BODY_MAX_CHARS, BODY_MAX_BYTES)
}

fn clean(raw: &str) -> Option<String> {
    let mut cleaned = String::with_capacity(raw.len());
    let mut pending_space = false;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            pending_space = true;
        } else if !dropped(ch) {
            if pending_space && !cleaned.is_empty() {
                cleaned.push(' ');
            }
            pending_space = false;
            cleaned.push(ch);
        }
    }
    (!cleaned.is_empty()).then_some(cleaned)
}

/// 不进通知的字符。空白在这之前已经单独处理（收成一个空格）。
fn dropped(ch: char) -> bool {
    ch.is_control()
        || matches!(
            ch,
            // 双向文字：阿拉伯字母标记、LRM/RLM、嵌入与覆盖、隔离
            '\u{061C}'
                | '\u{200E}'
                | '\u{200F}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
                // 看不见的格式字符：零宽空格、词连接符与不可见运算符、已废弃的格式控制、BOM、行间注释
                | '\u{200B}'
                | '\u{2060}'..='\u{2064}'
                | '\u{206A}'..='\u{206F}'
                | '\u{FEFF}'
                | '\u{FFF9}'..='\u{FFFB}'
                // 韩文填充符：常被用来拼出「看着是空白」的昵称
                | '\u{3164}'
                | '\u{FFA0}'
                // 非字符
                | '\u{FFFE}'
                | '\u{FFFF}'
        )
}

/// 把文字截到厂商字段的字数上限。按 UTF-16 码元计：厂商服务端多为 Java，这是
/// `String.length()` 的口径，汉字与字母各算 1，emoji 这类基本平面以外的字符算 2，比按字素簇计
/// 保守。截在字素簇之间，不会把代理对或 emoji 序列截成半个；截掉了内容时末尾是省略号，省略号
/// 算在上限里。
pub fn fit_utf16(text: &str, max_units: usize) -> String {
    let units = |part: &str| part.encode_utf16().count();
    if units(text) <= max_units {
        return text.to_string();
    }
    let ellipsis = units(ELLIPSIS);
    let mut out = String::with_capacity(text.len());
    let mut used = 0;
    for cluster in text.graphemes(true) {
        let width = units(cluster);
        if used + width + ellipsis > max_units {
            break;
        }
        out.push_str(cluster);
        used += width;
    }
    out.truncate(out.trim_end().len());
    if max_units >= ellipsis {
        out.push_str(ELLIPSIS);
    }
    out
}

/// 按字素簇截到 `max_chars` 个、`max_bytes` 字节以内；截掉了内容时末尾是省略号，省略号算在上限里。
fn truncate(text: &str, max_chars: usize, max_bytes: usize) -> String {
    let mut out = String::with_capacity(text.len().min(max_bytes));
    // 截断时保留到哪里：保留到这里再接一个省略号，个数与字节都还在上限内。
    let mut keep = 0;
    for (index, cluster) in text.graphemes(true).map(capped).enumerate() {
        if index == max_chars || out.len() + cluster.len() > max_bytes {
            out.truncate(keep);
            out.truncate(out.trim_end().len());
            out.push_str(ELLIPSIS);
            return out;
        }
        out.push_str(cluster);
        if index + 1 < max_chars && out.len() + ELLIPSIS.len() <= max_bytes {
            keep = out.len();
        }
    }
    out
}

/// 超长的字素簇只留第一个字符。
fn capped(cluster: &str) -> &str {
    if cluster.len() <= CLUSTER_MAX_BYTES {
        return cluster;
    }
    let first = cluster.chars().next().map_or(0, char::len_utf8);
    &cluster[..first]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chars(text: &str) -> usize {
        text.graphemes(true).count()
    }

    #[test]
    fn control_and_bidi_characters_are_removed_and_whitespace_collapsed() {
        assert_eq!(
            clean_body("  小明:\n\n今天\t几点\u{3000}开会  ").as_deref(),
            Some("小明: 今天 几点 开会")
        );
        // 换行伪装不出第二条通知；空字符、退格、删除、C1 控制字符直接去掉。
        assert_eq!(
            clean_body("你好\r\n系统通知：账号异常").as_deref(),
            Some("你好 系统通知：账号异常")
        );
        assert_eq!(
            clean_title("a\u{0}b\u{8}c\u{7F}d\u{9B}e").as_deref(),
            Some("abcde")
        );
        // 右到左覆盖能把扩展名倒着显示。
        assert_eq!(
            clean_body("invoice\u{202E}fdp.exe").as_deref(),
            Some("invoicefdp.exe")
        );
        for bidi in [
            '\u{061C}', '\u{200E}', '\u{200F}', '\u{202A}', '\u{202B}', '\u{202C}', '\u{202D}',
            '\u{202E}', '\u{2066}', '\u{2067}', '\u{2068}', '\u{2069}',
        ] {
            assert_eq!(
                clean_body(&format!("左{bidi}右")).as_deref(),
                Some("左右"),
                "{:04X}",
                bidi as u32
            );
        }
        // 去掉的字符两侧的空白仍然只算一个。
        assert_eq!(clean_body("a \u{202E} b").as_deref(), Some("a b"));
    }

    #[test]
    fn text_that_is_empty_after_cleaning_is_absent() {
        for blank in [
            "",
            "   ",
            "\n\t\r",
            "\u{200B}\u{FEFF}\u{202E}",
            "\u{3164}\u{3164}",
            "\u{0}\u{1B}",
        ] {
            assert_eq!(clean_title(blank), None, "{blank:?}");
            assert_eq!(clean_body(blank), None, "{blank:?}");
        }
    }

    /// 零宽连接符是 emoji 序列的一部分，不能当格式字符去掉。
    #[test]
    fn emoji_sequences_survive_cleaning() {
        let family = "👨\u{200D}👩\u{200D}👧\u{200D}👦";
        assert_eq!(clean_body(family).as_deref(), Some(family));
        let flag = "🇨🇳";
        assert_eq!(clean_body(flag).as_deref(), Some(flag));
    }

    #[test]
    fn titles_and_bodies_are_cut_with_an_ellipsis_inside_the_limit() {
        let title = "标".repeat(TITLE_MAX_CHARS);
        assert_eq!(clean_title(&title).as_deref(), Some(title.as_str()));
        let cut = clean_title(&"标".repeat(TITLE_MAX_CHARS + 1)).unwrap();
        assert_eq!(cut, format!("{}…", "标".repeat(TITLE_MAX_CHARS - 1)));
        assert_eq!(chars(&cut), TITLE_MAX_CHARS);

        let body = "正".repeat(BODY_MAX_CHARS);
        assert_eq!(clean_body(&body).as_deref(), Some(body.as_str()));
        let cut = clean_body(&"正".repeat(BODY_MAX_CHARS * 3)).unwrap();
        assert_eq!(cut, format!("{}…", "正".repeat(BODY_MAX_CHARS - 1)));
        assert_eq!(chars(&cut), BODY_MAX_CHARS);

        // 截在空格后面时，省略号贴着最后一个字，不留空格。
        let spaced = format!("{} {}", "a".repeat(TITLE_MAX_CHARS - 2), "b".repeat(10));
        assert_eq!(
            clean_title(&spaced).unwrap(),
            format!("{}…", "a".repeat(TITLE_MAX_CHARS - 2))
        );
    }

    /// 截断落在字素簇之间：emoji 序列、国旗、带变音符的字母不会被截成半个。
    #[test]
    fn truncation_never_splits_a_grapheme() {
        let family = "👨\u{200D}👩\u{200D}👧\u{200D}👦";
        let cut = clean_title(&family.repeat(TITLE_MAX_CHARS + 5)).unwrap();
        assert!(cut.ends_with('…'));
        assert!(
            cut.trim_end_matches('…')
                .graphemes(true)
                .all(|cluster| cluster == family),
            "{cut}"
        );
        assert!(cut.len() <= TITLE_MAX_BYTES);

        // 一面旗 8 字节：这里先碰到的是字节上限。
        let flags = "🇨🇳🇺🇸".repeat(BODY_MAX_CHARS);
        let cut = clean_body(&flags).unwrap();
        assert!(cut.ends_with('…'));
        assert!(chars(&cut) <= BODY_MAX_CHARS && cut.len() <= BODY_MAX_BYTES);
        assert!(
            cut.trim_end_matches('…')
                .graphemes(true)
                .all(|cluster| cluster == "🇨🇳" || cluster == "🇺🇸")
        );

        let accented = "e\u{301}".repeat(TITLE_MAX_CHARS + 1);
        let cut = clean_title(&accented).unwrap();
        assert_eq!(cut, format!("{}…", "e\u{301}".repeat(TITLE_MAX_CHARS - 1)));
    }

    /// 由多码点组成的字素簇按个数不长、按字节很长：字节上限保证载荷进得了厂商的大小限制。
    #[test]
    fn the_byte_limit_bounds_long_clusters() {
        // 一个家庭 emoji 25 字节：180 个是 4500 字节，超过 APNs 整个载荷的上限。
        let family = "👨\u{200D}👩\u{200D}👧\u{200D}👦";
        let cut = clean_body(&family.repeat(BODY_MAX_CHARS)).unwrap();
        assert!(cut.len() <= BODY_MAX_BYTES, "{}", cut.len());
        assert!(cut.ends_with('…'));
        assert!(
            cut.trim_end_matches('…')
                .graphemes(true)
                .all(|cluster| cluster == family)
        );
        let cut = clean_title(&family.repeat(TITLE_MAX_CHARS)).unwrap();
        assert!(cut.len() <= TITLE_MAX_BYTES, "{}", cut.len());

        // 叠满变音符的字只留底字。
        let zalgo = format!("a{}b", "\u{301}".repeat(200));
        assert_eq!(clean_title(&zalgo).as_deref(), Some("ab"));
    }

    /// 厂商字段按 UTF-16 码元截：汉字算 1，emoji 算 2，截在字素簇之间，省略号算在上限里。
    #[test]
    fn vendor_fields_are_cut_by_utf16_units_between_graphemes() {
        let units = |text: &str| text.encode_utf16().count();
        assert_eq!(fit_utf16("短标题", 50), "短标题");
        let exact = "标".repeat(50);
        assert_eq!(fit_utf16(&exact, 50), exact);
        let cut = fit_utf16(&"标".repeat(51), 50);
        assert_eq!(cut, format!("{}…", "标".repeat(49)));
        assert_eq!(units(&cut), 50);

        // 一个 emoji 是一个代理对：只留得下整个的。
        let cut = fit_utf16(&"😀".repeat(10), 6);
        assert_eq!(cut, "😀😀…");
        assert!(units(&cut) <= 6);
        let family = "👨\u{200D}👩\u{200D}👧\u{200D}👦";
        assert_eq!(units(family), 11);
        let cut = fit_utf16(&format!("{family}{family}"), 12);
        assert_eq!(cut, format!("{family}…"));
        let cut = fit_utf16(&format!("{family}{family}"), 11);
        assert_eq!(cut, "…", "整个字素簇连同省略号放不下");
        // 截在空格后面时，省略号贴着最后一个字。
        assert_eq!(fit_utf16("ab cdef", 4), "ab…");
        assert_eq!(fit_utf16("abc", 0), "");
    }

    /// 截过一次的文字拼上前缀再截一次，结果仍只有一个省略号、仍在上限内。
    #[test]
    fn fitting_a_composed_title_keeps_one_ellipsis() {
        let title = clean_title(&"群".repeat(100)).unwrap();
        let composed = fit_title(&format!("[3条] {title}"));
        assert_eq!(chars(&composed), TITLE_MAX_CHARS);
        assert!(composed.starts_with("[3条] 群"));
        assert_eq!(composed.matches('…').count(), 1);
        assert_eq!(fit_body("短正文"), "短正文");
    }
}
