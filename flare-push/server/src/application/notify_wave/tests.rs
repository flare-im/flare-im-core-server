use flare_server_core::error::ErrorCode;

use super::*;
use crate::domain::model::{DueClaim, WaveMessage, WaveTrigger};
use crate::domain::repository::NotifyGateStore;
use crate::infrastructure::notify::MemoryGateStore;

fn arrival<'a>(seq: u64, sender: &str, trigger: WaveTrigger) -> WaveArrival<'a> {
    WaveArrival {
        message: WaveMessage {
            message_id: format!("m-{seq}"),
            seq,
            sender_id: sender.to_string(),
        },
        class: 3,
        mentions_unknown: false,
        direct: &[],
        trigger,
    }
}

fn waves(store: Arc<MemoryGateStore>, fallback: Arc<MemoryGateStore>) -> NotifyWaves {
    NotifyWaves::new(store, fallback, Arc::new(NotifyPolicy::default()))
}

/// 领出这个存储里到期的波次。
async fn due_waves(store: &MemoryGateStore, now_ms: i64) -> Vec<WaveDue> {
    store
        .claim_due(now_ms, 100)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|claim| match claim {
            DueClaim::Wave(wave) => Some(wave),
            DueClaim::Slot(_) => None,
        })
        .collect()
}

/// 空闲会话当场枚举；间隔里的消息累加、存下最新一条的正文，到期时一起取出。
#[tokio::test]
async fn a_burst_is_held_and_released_once_with_the_newest_body() {
    let store = Arc::new(MemoryGateStore::primary());
    let waves = waves(store.clone(), Arc::new(MemoryGateStore::fallback()));
    let key = WaveKey::new("t1", "g-1");

    let WaveStep::Flush(first) = waves
        .arrive(1_000, &key, &arrival(1, "bob", WaveTrigger::Normal), b"one")
        .await
    else {
        panic!("an idle conversation enumerates now");
    };
    assert_eq!(first.batch.count, 1);
    assert_eq!(first.message.as_deref(), Some(&b"one"[..]));

    for (seq, body) in [(2, &b"two"[..]), (3, &b"three"[..])] {
        assert_eq!(
            waves
                .arrive(1_100, &key, &arrival(seq, "bob", WaveTrigger::Normal), body)
                .await,
            WaveStep::Held
        );
    }
    assert!(due_waves(&store, 3_999).await.is_empty(), "间隔还没结束");
    let due = due_waves(&store, 4_000).await;
    assert_eq!(due.len(), 1, "一个会话一个到期条目");
    let release = waves.due(4_000, &due[0]).await.expect("released");
    assert_eq!(release.batch.count, 2);
    assert_eq!(release.batch.newest.seq, 3);
    assert_eq!(release.message.as_deref(), Some(&b"three"[..]));
    assert!(
        waves.due(4_000, &due[0]).await.is_none(),
        "取过一次就没有了"
    );
}

/// 太大的正文不存：到期时没有正文，用通用文案。
#[tokio::test]
async fn an_oversized_body_is_not_kept() {
    let store = Arc::new(MemoryGateStore::primary());
    let waves = waves(store.clone(), Arc::new(MemoryGateStore::fallback()));
    let key = WaveKey::new("t1", "g-1");
    waves
        .arrive(0, &key, &arrival(1, "bob", WaveTrigger::Normal), b"one")
        .await;
    let big = vec![7u8; MAX_STORED_MESSAGE_BYTES + 1];
    waves
        .arrive(10, &key, &arrival(2, "bob", WaveTrigger::Normal), &big)
        .await;
    let due = due_waves(&store, 3_000).await;
    let release = waves.due(3_000, &due[0]).await.expect("released");
    assert_eq!(release.batch.newest.seq, 2);
    assert_eq!(release.message, None);
}

/// 推送 Redis 不可用：改用进程内的状态，到期条目登记在那里，按来源回到那里处理。
#[tokio::test]
async fn an_unavailable_store_falls_back_to_the_in_process_waves() {
    struct Down;

    #[async_trait::async_trait]
    impl WaveStore for Down {
        async fn load_wave(&self, _key: &WaveKey) -> Result<WaveSnapshot, FlareError> {
            Err(FlareError::localized(ErrorCode::ServiceUnavailable, "down"))
        }
        async fn commit_wave(&self, _commit: &WaveCommit) -> Result<bool, FlareError> {
            Err(FlareError::localized(ErrorCode::ServiceUnavailable, "down"))
        }
    }

    let fallback = Arc::new(MemoryGateStore::fallback());
    let waves = NotifyWaves::new(
        Arc::new(Down),
        fallback.clone(),
        Arc::new(NotifyPolicy::default()),
    );
    let key = WaveKey::new("t1", "g-1");
    assert!(matches!(
        waves
            .arrive(0, &key, &arrival(1, "bob", WaveTrigger::Normal), b"one")
            .await,
        WaveStep::Flush(_)
    ));
    assert_eq!(
        waves
            .arrive(10, &key, &arrival(2, "bob", WaveTrigger::Normal), b"two")
            .await,
        WaveStep::Held,
        "进程内的状态照样把间隔里的消息攒起来"
    );
    let due = due_waves(&fallback, i64::MAX).await;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].origin, GateStoreOrigin::Fallback);
    assert_eq!(
        waves
            .due(3_000, &due[0])
            .await
            .expect("released")
            .batch
            .count,
        1
    );
}

/// 一直冲突：放行，这一条当场枚举，只推它自己；超过成员上限的 @所有人 不因此全员扇出。
#[tokio::test]
async fn contention_enumerates_just_this_message() {
    struct AlwaysConflicts;

    #[async_trait::async_trait]
    impl WaveStore for AlwaysConflicts {
        async fn load_wave(&self, _key: &WaveKey) -> Result<WaveSnapshot, FlareError> {
            Ok(WaveSnapshot::default())
        }
        async fn commit_wave(&self, _commit: &WaveCommit) -> Result<bool, FlareError> {
            Ok(false)
        }
    }

    let waves = NotifyWaves::new(
        Arc::new(AlwaysConflicts),
        Arc::new(MemoryGateStore::fallback()),
        Arc::new(NotifyPolicy::default()),
    );
    let key = WaveKey::new("t1", "g-1");
    let all = WaveTrigger::AllEveryone {
        interval_ms: waves.all_mention_interval_ms("t1"),
    };
    let WaveStep::Flush(release) = waves.arrive(0, &key, &arrival(9, "bob", all), b"x").await
    else {
        panic!("fails open");
    };
    assert_eq!(release.batch.count, 1);
    assert_eq!(release.batch.mention_all, MentionAll::No);
    assert_eq!(release.message.as_deref(), Some(&b"x"[..]));
}

/// 超过成员上限的 @所有人 按会话限频：配额内全员，配额外按普通消息。
#[tokio::test]
async fn mention_all_quota_is_kept_in_the_wave_state() {
    let waves = waves(
        Arc::new(MemoryGateStore::primary()),
        Arc::new(MemoryGateStore::fallback()),
    );
    let key = WaveKey::new("t1", "big");
    let all = WaveTrigger::AllEveryone {
        interval_ms: waves.all_mention_interval_ms("t1"),
    };
    let first = waves
        .arrive(1_000, &key, &arrival(1, "bob", all), b"a")
        .await;
    assert_eq!(first.mention_all(), MentionAll::Everyone);
    let second = waves
        .arrive(61_000, &key, &arrival(2, "bob", all), b"b")
        .await;
    assert_eq!(
        second.mention_all(),
        MentionAll::No,
        "十分钟内第二次按普通消息处理"
    );
}
