//! postgres_repository 的测试（外联自主文件，仓库测试外联惯例）。
//!
//! 源码自检断言（include_str!）读取的是 mod.rs（生产源码）。

// 外联后模块层级多了一层 tests：内联测试模块的 `use super::X` 现在指向本文件，
// 经此转发拿到 mod.rs（postgres_repository 模块）的顶层项。
#[cfg(test)]
use super::{
    clamp_participant_page_limit, encode_participant_cursor, fill_single_chat_peers,
    last_message_preview_from_content, parse_participant_cursor, ParticipantPageCursor,
    MAX_PARTICIPANT_PAGE_LIMIT, MAX_RECOMPUTED_UNREAD,
};

#[cfg(test)]
mod last_message_preview_tests {
    use super::last_message_preview_from_content;
    use flare_proto::common::{
        ImageContent, MessageContent, TextContent, message_content::Content,
    };
    use prost::Message as _;

    fn encode(content: Content) -> Vec<u8> {
        MessageContent {
            content: Some(content),
        }
        .encode_to_vec()
    }

    #[test]
    fn text_content_yields_text_preview() {
        let bytes = encode(Content::Text(TextContent {
            text: "hello preview".to_string(),
            mentions: vec![],
        }));
        let (preview, kind) = last_message_preview_from_content(Some(&bytes));
        assert_eq!(preview.as_deref(), Some("hello preview"));
        assert_eq!(kind.as_deref(), Some("text"));
    }

    #[test]
    fn media_content_yields_label() {
        let bytes = encode(Content::Image(ImageContent::default()));
        let (preview, kind) = last_message_preview_from_content(Some(&bytes));
        assert_eq!(preview.as_deref(), Some("[图片]"));
        assert_eq!(kind.as_deref(), Some("image"));
    }

    #[test]
    fn empty_or_invalid_content_yields_none() {
        assert_eq!(last_message_preview_from_content(None), (None, None));
        assert_eq!(last_message_preview_from_content(Some(&[])), (None, None));
        // 无效 protobuf(非法 wire type)→ 解码失败 → None。
        assert_eq!(
            last_message_preview_from_content(Some(&[0xff, 0xff])),
            (None, None)
        );
    }

    /// SQL 的 SELECT 列与 `row.get("...")` 的列名必须在**同一个函数内**对得上。
    ///
    /// 2026-08-16 出过一次：`list_conversation_participants` 的映射读了
    /// `row.get("mention_only")`，而同一函数的 SELECT 没有列它——加字段时改了结构体、
    /// 漏了 SQL（同批加入的 `visible_from_seq` 两处都补了）。
    ///
    /// 代价远超一次查询失败：`row.get` 缺列是 **panic** 不是 Err，打挂 tokio worker 后，
    /// 同一服务上**别的**在途 gRPC 调用会被连带打断成 `h2 protocol error`，
    /// 而调用方（网关扩散）按 best-effort 吞掉——实时投递就这样悄悄降级，
    /// 且是时序性的、不稳定复现，靠跑 E2E 抓不住。
    ///
    /// 所以守卫落在源码层：不连库、确定性、几毫秒。
    /// **按函数切分是关键**——本文件另有 3 处正确的 `AS mention_only`，
    /// 全局搜索会被它们盖过去，照样放行这个 bug。
    #[test]
    fn row_get_columns_are_selected_in_the_same_function() {
        let src = include_str!("mod.rs");
        // 只看生产代码：测试模块里满是示例字符串，会造成噪声误报。
        let src = src.split("\n#[cfg(test)]").next().unwrap_or(src);

        let get_call = regex_lite_find_get_calls;

        let mut chunks: Vec<(String, String)> = vec![("<file head>".into(), String::new())];
        for line in src.lines() {
            let trimmed = line.trim_start();
            let is_method = line.starts_with("    ")
                && !line.starts_with("     ")
                && (trimmed.starts_with("fn ")
                    || trimmed.starts_with("async fn ")
                    || trimmed.starts_with("pub fn ")
                    || trimmed.starts_with("pub async fn "));
            if is_method {
                let name = trimmed
                    .trim_start_matches("pub ")
                    .trim_start_matches("async ")
                    .trim_start_matches("fn ")
                    .split('(')
                    .next()
                    .unwrap_or("?")
                    .to_string();
                chunks.push((name, String::new()));
            }
            let body = &mut chunks.last_mut().expect("at least one chunk").1;
            body.push_str(line);
            body.push('\n');
        }

        let mut problems = Vec::new();
        for (fname, chunk) in &chunks {
            let wanted = get_call(chunk);
            if wanted.is_empty() {
                continue;
            }
            // 只跟本函数内联的 SQL 比对（见 sql_literals 的注释）。
            let sql = sql_literals(chunk);
            if sql.is_empty() {
                continue; // 本函数没有内联 SQL，无从比对
            }
            for col in wanted {
                if !sql.contains(&col) {
                    problems.push(format!(
                        "{fname}: row.get(\"{col}\") 但同函数的 SQL 里没有这一列"
                    ));
                }
            }
        }

        assert!(
            problems.is_empty(),
            "SELECT 与 row.get 的列名漂移（改结构体时漏改 SQL）：\n{}",
            problems.join("\n")
        );
    }

    /// 取出一段代码里所有 `.get("X")` 的 X。
    fn regex_lite_find_get_calls(chunk: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = chunk;
        while let Some(at) = rest.find(".get(\"") {
            let after = &rest[at + 6..];
            match after.find('"') {
                Some(end) => {
                    out.push(after[..end].to_string());
                    rest = &after[end..];
                }
                None => break,
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// 只取这段代码里的 **SQL 原始字符串**（`r#"..."#`）拼在一起。
    ///
    /// 不能拿整段 Rust 正文来找列名：结构体字面量写的是
    /// `mention_only: row.get("mention_only")`，字段名本身就含该词，
    /// 于是「SQL 里没选」也会被自己证明成「选了」——第一版就栽在这儿，
    /// 把列删掉测试照样绿。
    fn sql_literals(chunk: &str) -> String {
        let mut out = String::new();
        let mut rest = chunk;
        while let Some(at) = rest.find("r#\"") {
            let after = &rest[at + 3..];
            match after.find("\"#") {
                Some(end) => {
                    out.push_str(&after[..end]);
                    out.push('\n');
                    rest = &after[end + 2..];
                }
                None => break,
            }
        }
        out
    }
}


#[cfg(test)]
mod participant_page_limit_tests {
    use super::{
        MAX_PARTICIPANT_PAGE_LIMIT, ParticipantPageCursor, clamp_participant_page_limit,
        encode_participant_cursor, parse_participant_cursor,
    };
    use chrono::{DateTime, Utc};

    fn at(micros: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_micros(micros).expect("有效时间戳")
    }

    #[test]
    fn cursor_round_trips_even_when_user_id_contains_colons() {
        // user_id 含冒号是最容易把游标解析写错的一类输入
        for user_id in ["u1", "u:1", "a:b:c:d", ":", "用户:1"] {
            let encoded = encode_participant_cursor(at(1_700_000_000_000_000), user_id, 42, 7);
            let parsed = parse_participant_cursor(Some(&encoded));
            assert_eq!(
                parsed.keyset,
                Some((at(1_700_000_000_000_000), user_id.to_string())),
                "user_id={user_id:?} 往返失败"
            );
            assert_eq!(parsed.carried, Some((42, 7)));
            assert_eq!(parsed.legacy_offset, 0);
        }
    }

    #[test]
    fn legacy_numeric_cursor_still_works() {
        // 升级过程中在途的分页请求仍会带旧的纯数字游标
        let parsed = parse_participant_cursor(Some("1500"));
        assert_eq!(parsed.legacy_offset, 1500);
        assert!(parsed.keyset.is_none());
        assert!(
            parsed.carried.is_none(),
            "旧游标没有带 total/version，必须重算"
        );
        assert_eq!(parse_participant_cursor(Some("-5")).legacy_offset, 0);
    }

    #[test]
    fn broken_cursor_falls_back_to_first_page_not_a_random_offset() {
        // 损坏游标若退成某个 offset，会静默跳过一段成员且无任何错误信号
        for bad in ["k:", "k:abc:1:2:u1", "k:1:2:u1", "k:1:2:3:", "k:1:2:3"] {
            assert_eq!(
                parse_participant_cursor(Some(bad)),
                ParticipantPageCursor::default(),
                "游标 {bad:?} 应退回首页"
            );
        }
        assert_eq!(
            parse_participant_cursor(None),
            ParticipantPageCursor::default()
        );
        assert_eq!(
            parse_participant_cursor(Some("  ")),
            ParticipantPageCursor::default()
        );
    }

    #[test]
    fn unread_recount_must_stay_capped() {
        use super::MAX_RECOMPUTED_UNREAD;
        assert!(
            (1..=10_000).contains(&MAX_RECOMPUTED_UNREAD),
            "上限须在合理区间：太小会让未读显示失真，太大等于没封顶"
        );

        // 只断言常量不够——把 SQL 里的 LIMIT 删掉常量照样在。直接对 SQL 断言。
        let source = include_str!("mod.rs");
        let lateral = source
            .split("LEFT JOIN LATERAL (\n                -- 未读数**封顶**再计数")
            .nth(1)
            .expect("未读 LATERAL 存在")
            .split(") unread_tail ON TRUE")
            .next()
            .expect("LATERAL 结束");
        assert!(
            lateral.contains("LIMIT $4"),
            "未读重算必须封顶，否则对整条时间线做 COUNT（实测 845K 未读时 304ms/次）"
        );
        assert!(
            lateral.contains("FROM (") && lateral.contains(") capped"),
            "必须先 LIMIT 成子查询再 COUNT；直接给 COUNT 加 LIMIT 不起作用"
        );
    }

    #[test]
    fn apply_message_event_large_group_probe_must_be_bounded() {
        // 发送热路径判定大群不得对全群做无界 COUNT(*):10 万群每条消息会 O(成员) 扫描
        // (实测陈旧统计下病态计划 39 分钟钉核)。必须用「LIMIT 阈值+1」有界探测,读至多 阈值+1 行。
        // 生产代码分布在 mod.rs 与 helpers.rs，自检源码必须拼接两者，
        // 否则搬进 helpers 的实现会漏检。
        let source = format!(
            "{}{}",
            include_str!("mod.rs"),
            include_str!("helpers.rs")
        );
        // 运行时拼接 needle,避免本断言的字面量被 include_str! 读进来命中自己。
        let unbounded_count = format!("SELECT COUNT(*)::{} AS member_count", "INT");
        assert!(
            !source.contains(&unbounded_count),
            "apply_message_event 不得用无界 member_stats COUNT 判定大群规模"
        );
        assert!(
            source.contains("Failed to probe conversation size"),
            "大群判定须用有界 LIMIT 探测块(读至多 阈值+1 行),该探测必须存在"
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)] // 钉的就是常量的量级
    fn page_limit_stays_large_enough_to_avoid_hundreds_of_round_trips() {
        // 回归门禁：上限若掉回 500，十万人群的成员遍历会退回 200 次串行往返
        // （实测一条已读回执 47 秒）。
        assert!(
            MAX_PARTICIPANT_PAGE_LIMIT >= 5000,
            "页大小上限不能调小：十万人群会退回数百次串行往返"
        );
        // 十万人群的往返次数必须控制在 20 次量级
        let round_trips = (100_000 + MAX_PARTICIPANT_PAGE_LIMIT - 1) / MAX_PARTICIPANT_PAGE_LIMIT;
        assert!(round_trips <= 20, "十万人群往返次数 {round_trips} 次，过多");

        assert_eq!(clamp_participant_page_limit(0), 1);
        assert_eq!(clamp_participant_page_limit(-5), 1);
        assert_eq!(clamp_participant_page_limit(500), 500);
        assert_eq!(clamp_participant_page_limit(5000), 5000);
        assert_eq!(
            clamp_participant_page_limit(99_999),
            MAX_PARTICIPANT_PAGE_LIMIT
        );
    }
}

#[cfg(test)]
mod single_chat_peer_tests {
    use super::fill_single_chat_peers;
    use crate::domain::model::{ConversationSummary, ConversationType};
    use std::collections::HashMap;

    fn summary(conversation_id: &str, display_name: Option<&str>) -> ConversationSummary {
        ConversationSummary {
            conversation_id: conversation_id.to_string(),
            conversation_type: ConversationType::Single,
            business_type: None,
            last_message_id: None,
            last_message_time: None,
            last_sender_id: None,
            last_message_type: None,
            last_content_type: None,
            last_message_preview: None,
            unread_count: 0,
            last_read_seq: 0,
            metadata: HashMap::new(),
            server_cursor_ts: None,
            display_name: display_name.map(str::to_string),
            last_message_seq: None,
            channel_id: String::new(),
            participant_version: 0,
            member_preview: Vec::new(),
            is_muted: false,
            is_mention_only: false,
            notify_all: false,
            is_pinned: false,
            is_archived: false,
            settings_version: 0,
            draft: None,
            visible_after_seq: 0,
        }
    }

    /// 对端只进 `channel_id`；没有会话名的单聊名字仍为空，账号不当名字。
    #[test]
    fn peer_fills_channel_id_but_never_the_display_name() {
        let peers = HashMap::from([
            ("1A-unnamed".to_string(), "u-peer".to_string()),
            ("1A-blank".to_string(), "u-blank".to_string()),
            ("1A-named".to_string(), "u-named".to_string()),
        ]);
        let mut summaries = vec![
            summary("1A-unnamed", None),
            summary("1A-blank", Some("  ")),
            summary("1A-named", Some("Alice")),
            summary("1A-no-peer", None),
        ];

        fill_single_chat_peers(&mut summaries, &peers);

        let got: Vec<(&str, Option<&str>)> = summaries
            .iter()
            .map(|s| (s.channel_id.as_str(), s.display_name.as_deref()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("u-peer", None),
                ("u-blank", Some("  ")),
                ("u-named", Some("Alice")),
                ("", None),
            ]
        );
    }
}

#[cfg(test)]
mod recent_unread_sql_tests {
    /// 去掉 SQL 注释与多余空白，便于逐字比较两段 SQL。
    fn normalized(sql: &str) -> String {
        sql.lines()
            .map(|line| line.split("--").next().unwrap_or_default())
            .flat_map(str::split_whitespace)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn production_source() -> &'static str {
        let source = include_str!("mod.rs");
        source.split("\n#[cfg(test)]").next().unwrap_or(source)
    }

    /// 生产代码里每个 `unread_tail` LATERAL 的正文，按出现顺序。
    fn unread_laterals(source: &str) -> Vec<String> {
        let pieces: Vec<&str> = source.split(") unread_tail ON TRUE").collect();
        pieces[..pieces.len() - 1]
            .iter()
            .map(|piece| {
                let start = piece
                    .rfind("LEFT JOIN LATERAL (")
                    .expect("unread_tail 是一个 LEFT JOIN LATERAL");
                normalized(&piece[start..])
            })
            .collect()
    }

    /// 生产代码里每个未读 CASE 表达式，按出现顺序。
    fn unread_cases(source: &str) -> Vec<String> {
        // 运行时拼接起止标记，避免本函数的字面量被 include_str! 读进来命中自己。
        let start = format!(
            "CASE WHEN GREATEST(sp.history_cleared_through_seq, {}) > 0",
            "s.history_cleared_through_seq"
        );
        let end = format!("END AS {}", "unread_count");
        source
            .split(start.as_str())
            .skip(1)
            .map(|rest| normalized(rest.split(end.as_str()).next().unwrap_or_default()))
            .collect()
    }

    /// 「有新未读消息的会话」与会话列表摘要用的必须是同一个未读定义：已物化的未读与按已读水位
    /// 重算（封顶）的未读取大者，重算时排除本人发的、已撤回已删除的、看不到的消息。
    ///
    /// 两处各写一份 SQL，靠这条用例保证它们逐字相同：只改一处的话，会话列表说「没有未读」的
    /// 会话照样会被补一条推送提示（或者反过来），而这种不一致只在特定数据上才露头。
    #[test]
    fn recent_unread_uses_the_conversation_list_unread_definition() {
        let source = production_source();

        let laterals = unread_laterals(source);
        assert_eq!(
            laterals.len(),
            2,
            "未读重算应当只有会话摘要与新未读会话这两处；多了一处就把它也纳入比较"
        );
        assert_eq!(laterals[0], laterals[1], "未读重算的 SQL 在两处不一致");
        assert!(
            laterals[1].contains("LIMIT $4") && laterals[1].contains(") capped"),
            "未读重算必须封顶，否则对整条时间线做 COUNT"
        );

        let cases = unread_cases(source);
        assert_eq!(cases.len(), 2, "未读 CASE 表达式应当只有两处");
        assert_eq!(cases[0], cases[1], "未读 CASE 表达式在两处不一致");
    }

    /// 这条读按用户扫参与者行，必须先用窗口探针裁掉没有新消息的会话，并且只回答在席的会话。
    #[test]
    fn recent_unread_prunes_by_the_time_window_and_reads_seated_conversations_only() {
        let source = production_source();
        let body = source
            .split("async fn list_recent_unread_conversations")
            .nth(1)
            .expect("函数应存在");
        let body = normalized(&body[..body.find("\n    async fn ").unwrap_or(body.len())]);

        assert!(
            body.contains("mx.timestamp > $3"),
            "缺了窗口探针就要为这个用户的每个会话各算一遍最后一条消息与未读"
        );
        assert!(
            body.contains("AND NOT COALESCE(sp.is_deleted, false) AND sp.quit_at IS NULL"),
            "只回答在席（未隐藏、未退出）的会话：隐藏了会话的人不再收到它的推送"
        );
        assert!(
            body.contains("AND message_tail.last_sender_id <> $2"),
            "自己发的最后一条消息不该给自己补提示"
        );
        assert!(body.contains("LIMIT $6"), "结果必须有上限");
    }
}
