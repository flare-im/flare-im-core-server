## 改了什么

<!-- 一两句说清楚。**为什么这么改**比改了什么更重要——审阅时最缺的就是这个。 -->

## 怎么验证的

<!--
跑过的命令与结果。CONTRIBUTING 里那条门禁命令是底线：
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test

只写「本地测过」帮不上忙——贴上实际输出或说明测了哪条路径。
-->

## 检查项

- [ ] 门禁命令本地跑过
- [ ] 加了能覆盖这个改动的测试（或说明为什么不需要）
- [ ] 改了协议 / 契约的话，相关仓库已同步（proto、bindings、各端 SDK）
- [ ] 加了表或列的话，`init.sql` 与 `migrations/` **两处都改了**
