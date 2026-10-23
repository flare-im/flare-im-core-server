//! Read receipts of a group's messages, as their sender sees them: how many members have read a
//! message ("N unread / all read") and who they are.
//!
//! **Source.** A member's read position is their read watermark: the `last_read_seq` that read
//! receipts advance (`mark_as_read`). A member has read a message when their watermark is at or past
//! its seq. Only members who can see the message count: a member whose history floor
//! (`visible_from_seq`) is at or past the message's seq never sees it.
//!
//! **Members.** A member is a participant who has not left. One who hid the conversation (removed
//! it from their list) still is: hiding is how their list shows it. Hiding is not reading: it
//! clears their own unread count but leaves their read position where their receipts put it, so
//! what they had not read stays unread here (deleting a friend or blocking someone hides the chat
//! on one side, and the other side must not see that as "all read").
//!
//! **Who may ask.** Only a message's sender learns who read it. Someone else asking about it is
//! answered as for a message that does not exist, so a member cannot tell another sender's message
//! (or one from before they joined) from an id that names nothing.
//!
//! **Bound.** Receipts are answered for groups of at most [`READ_RECEIPT_MEMBER_LIMIT`]
//! members. Above it (and for every conversation that is not a group) the answer is "unavailable"
//! and clients show nothing: counting a 100,000-member group per message would put an O(members)
//! read on every visible page. The limit is the default of the large-conversation threshold under
//! which unread counts are kept precise (`large_conversation_precise_unread_threshold`), so one
//! group size decides both.
//!
//! **Cost.** One bounded read of at most `limit + 1` member watermarks (the primary key order, so a
//! 100,000-member group reads 501 rows and stops) and one index lookup of the batch's messages.
//! The watermarks are sorted once; each message is then counted with binary searches:
//! O(M log M + B log M) for M members and B messages, with M <= 500 and B <= 50.

/// Groups with more members than this get no read receipts.
pub const READ_RECEIPT_MEMBER_LIMIT: usize = 500;
/// The most messages one summary request may name (a visible page).
pub const MAX_READ_SUMMARY_MESSAGES: usize = 50;
/// Reader page size when the request names none.
pub const DEFAULT_READER_PAGE: usize = 50;
/// The largest reader page.
pub const MAX_READER_PAGE: usize = 200;
/// Message statuses that end a message's receipts (`MessageStatus`): recalled and deleted.
const ENDED_STATUSES: [i32; 2] = [
    flare_proto::common::MessageStatus::Recalled as i32,
    flare_proto::common::MessageStatus::Deleted as i32,
];

/// One member's read watermark and history floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberReadPosition {
    pub user_id: String,
    /// The member has read every message with a seq at or below this.
    pub last_read_seq: i64,
    /// The member sees only messages with a seq above this (0: the whole history).
    pub visible_from_seq: i64,
}

/// The stored facts of a message that its receipts need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptMessage {
    pub server_id: String,
    pub seq: i64,
    pub sender_id: String,
    pub status: i32,
}

impl ReceiptMessage {
    /// Whether `sender` sent this message and it still has receipts (not recalled or deleted).
    pub fn has_receipts_for(&self, sender: &str) -> bool {
        self.seq > 0 && self.sender_id == sender && !ENDED_STATUSES.contains(&self.status)
    }
}

/// How many members have read one message. `total = read_count + unread_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadCounts {
    pub read_count: u32,
    pub unread_count: u32,
    pub total: u32,
}

/// One message's read counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageReadSummary {
    pub server_msg_id: String,
    pub counts: ReadCounts,
}

/// The summaries of a batch: `available == false` (not a group, or above the limit) carries none.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MessageReadSummaries {
    pub available: bool,
    pub summaries: Vec<MessageReadSummary>,
}

/// Which side of a message's readers a page lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderState {
    Read,
    Unread,
}

/// One page of a message's readers (or non-readers), in `user_id` order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MessageReaderPage {
    pub available: bool,
    /// Both sides' counts; `None` when unavailable.
    pub counts: Option<ReadCounts>,
    pub user_ids: Vec<String>,
    /// The cursor of the next page; empty on the last.
    pub next_cursor: String,
    pub has_more: bool,
}

/// The read watermarks of a group's members other than one sender, indexed for counting.
#[derive(Debug, Clone)]
pub struct ReadWatermarks {
    /// Members other than the sender, by `user_id`.
    members: Vec<MemberReadPosition>,
    /// Watermarks, ascending.
    read: Vec<i64>,
    /// History floors, ascending.
    floors: Vec<i64>,
    /// `min(watermark, floor)` per member, ascending: the members whose watermark and floor are
    /// both at or past a seq have read past a message they cannot see.
    hidden_reads: Vec<i64>,
}

impl ReadWatermarks {
    /// The watermarks of `members` except `sender`.
    pub fn without_sender(members: Vec<MemberReadPosition>, sender: &str) -> Self {
        let mut members: Vec<_> = members
            .into_iter()
            .filter(|member| member.user_id != sender)
            .collect();
        members.sort_by(|a, b| a.user_id.cmp(&b.user_id));
        members.dedup_by(|a, b| a.user_id == b.user_id);
        let mut read: Vec<i64> = members.iter().map(|m| m.last_read_seq).collect();
        let mut floors: Vec<i64> = members.iter().map(|m| m.visible_from_seq).collect();
        let mut hidden_reads: Vec<i64> = members
            .iter()
            .map(|m| m.last_read_seq.min(m.visible_from_seq))
            .collect();
        read.sort_unstable();
        floors.sort_unstable();
        hidden_reads.sort_unstable();
        Self {
            members,
            read,
            floors,
            hidden_reads,
        }
    }

    /// How many of `sorted` are at or past `seq`.
    fn at_or_past(sorted: &[i64], seq: i64) -> usize {
        sorted.len() - sorted.partition_point(|value| *value < seq)
    }

    /// The read counts of the message at `seq`: O(log M).
    pub fn counts(&self, seq: i64) -> ReadCounts {
        // Members who see it: floor below the seq.
        let total = self.floors.partition_point(|floor| *floor < seq);
        // Read and visible = read - (read and hidden): #{floor < seq <= watermark}.
        let read = Self::at_or_past(&self.read, seq) - Self::at_or_past(&self.hidden_reads, seq);
        ReadCounts {
            read_count: read as u32,
            unread_count: (total - read) as u32,
            total: total as u32,
        }
    }

    /// One page of the members on `state`'s side of the message at `seq`, after `cursor` (a
    /// `user_id` of the previous page; empty for the first page): O(M).
    pub fn page(
        &self,
        seq: i64,
        state: ReaderState,
        cursor: &str,
        limit: usize,
    ) -> (Vec<String>, String, bool) {
        let mut side = self
            .members
            .iter()
            .filter(|member| member.visible_from_seq < seq)
            .filter(|member| match state {
                ReaderState::Read => member.last_read_seq >= seq,
                ReaderState::Unread => member.last_read_seq < seq,
            })
            .filter(|member| cursor.is_empty() || member.user_id.as_str() > cursor)
            .map(|member| member.user_id.clone());
        let page: Vec<String> = side.by_ref().take(limit).collect();
        let has_more = side.next().is_some();
        let next_cursor = if has_more {
            page.last().cloned().unwrap_or_default()
        } else {
            String::new()
        };
        (page, next_cursor, has_more)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(user_id: &str, last_read_seq: i64, visible_from_seq: i64) -> MemberReadPosition {
        MemberReadPosition {
            user_id: user_id.into(),
            last_read_seq,
            visible_from_seq,
        }
    }

    /// The counts by binary search equal a member-by-member count, for every seq around every
    /// watermark and floor of a mixed group.
    #[test]
    fn counts_equal_a_member_by_member_count() {
        let members = vec![
            member("sender", 100, 0),
            member("a", 10, 0),
            member("b", 12, 0),
            member("c", 0, 0),
            member("d", 20, 11),
            member("e", 5, 15),
            member("f", 30, 30),
            member("g", 11, 0),
        ];
        let marks = ReadWatermarks::without_sender(members.clone(), "sender");
        for seq in 0..=35 {
            let others = members.iter().filter(|m| m.user_id != "sender");
            let visible: Vec<_> = others.filter(|m| m.visible_from_seq < seq).collect();
            let read = visible.iter().filter(|m| m.last_read_seq >= seq).count() as u32;
            let total = visible.len() as u32;
            assert_eq!(
                marks.counts(seq),
                ReadCounts {
                    read_count: read,
                    unread_count: total - read,
                    total,
                },
                "seq {seq}"
            );
        }
    }

    #[test]
    fn the_sender_never_counts_and_a_lone_sender_has_nobody_to_read() {
        let marks = ReadWatermarks::without_sender(vec![member("sender", 0, 0)], "sender");
        assert_eq!(marks.counts(7), ReadCounts::default());
        let marks = ReadWatermarks::without_sender(
            vec![member("sender", 0, 0), member("peer", 7, 0)],
            "sender",
        );
        assert_eq!(
            marks.counts(7),
            ReadCounts {
                read_count: 1,
                unread_count: 0,
                total: 1,
            }
        );
        assert_eq!(marks.counts(8).unread_count, 1);
    }

    /// Pages list one side in `user_id` order; the cursor resumes after the last id; the two sides
    /// partition the members who see the message.
    #[test]
    fn reader_pages_partition_the_members_who_see_the_message() {
        let mut members: Vec<_> = (0..7)
            .map(|i| member(&format!("u{i}"), if i % 2 == 0 { 9 } else { 3 }, 0))
            .collect();
        members.push(member("late", 9, 9));
        members.push(member("sender", 9, 0));
        let marks = ReadWatermarks::without_sender(members, "sender");

        let mut read = Vec::new();
        let mut cursor = String::new();
        loop {
            let (page, next, has_more) = marks.page(5, ReaderState::Read, &cursor, 2);
            assert!(page.len() <= 2);
            read.extend(page);
            if !has_more {
                assert!(next.is_empty());
                break;
            }
            cursor = next;
        }
        assert_eq!(read, ["u0", "u2", "u4", "u6"]);
        let (unread, _, has_more) = marks.page(5, ReaderState::Unread, "", 10);
        assert_eq!(unread, ["u1", "u3", "u5"]);
        assert!(!has_more);
        let counts = marks.counts(5);
        assert_eq!((counts.read_count, counts.unread_count), (4, 3));
        // A full last page has no next page.
        let (page, next, has_more) = marks.page(5, ReaderState::Unread, "", 3);
        assert_eq!((page.len(), next.as_str(), has_more), (3, "", false));
    }

    #[test]
    fn only_the_senders_live_messages_have_receipts() {
        let message = |sender: &str, status: i32, seq: i64| ReceiptMessage {
            server_id: "m".into(),
            seq,
            sender_id: sender.into(),
            status,
        };
        assert!(message("alice", 2, 4).has_receipts_for("alice"));
        assert!(!message("bob", 2, 4).has_receipts_for("alice"));
        for ended in ENDED_STATUSES {
            assert!(!message("alice", ended, 4).has_receipts_for("alice"));
        }
        assert!(!message("alice", 2, 0).has_receipts_for("alice"));
    }
}
