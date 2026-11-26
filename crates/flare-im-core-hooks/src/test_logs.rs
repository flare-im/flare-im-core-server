//! 测试里抓日志：把当前线程的 tracing 事件按文本格式（无颜色、无时间）写进内存，供断言日志行。

use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 在本线程抓 `level` 及以上的日志，直到 [`LogCapture::finish`]。
pub(crate) struct LogCapture {
    buffer: Buffer,
    guard: tracing::subscriber::DefaultGuard,
    _second: tracing::Dispatch,
}

impl LogCapture {
    pub(crate) fn start(level: tracing::Level) -> Self {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        // 进程里只有一个调度器时，tracing 按第一次走到某个日志调用点的线程决定它开不开：别的测试
        // 线程先走到就会把它缓存成关闭，这里便收不到。多登记一个不输出的调度器，兴趣改按全部调度器
        // 计算，再整体重算一次。
        let second = tracing::Dispatch::new(tracing_subscriber::registry());
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .without_time()
                .with_max_level(level)
                .finish(),
        );
        tracing::callsite::rebuild_interest_cache();
        Self {
            buffer,
            guard,
            _second: second,
        }
    }

    /// 停止抓取，返回抓到的全部文本。
    pub(crate) fn finish(self) -> String {
        drop(self.guard);
        String::from_utf8(self.buffer.0.lock().unwrap().clone()).unwrap()
    }
}
