# StoreSystem — Agent Development Contract

本项目使用 **superpowers 6.0** 方法论管理。所有 AI agent 必须遵循以下规则。

## 核心铁律

1. **禁止重写现有代码。** 改动一律用 `edit_file`，最小 diff。
2. **增强而非替换。** 在现有接口上扩展，不改变调用方签名。
3. **稳定优先。** 每改动一个模块 → 编译 → 测试 → 通过才继续。
4. **有序迭代。** 每次只做一个功能点，用 `v0.1.X` 标签。
5. **设计文档先行。** 任何新功能必须先写 `docs/superpowers/specs/` 设计文档。
6. **淘汰只限于设计真正不合理时。** 在 commit 中论证"为什么必须废弃"。
7. **文档代码同步。** 每次提交前 README.md 必须与代码同步，禁止代码和文档不一致。

## 技能工作流

以下技能在适当场景下自动触发。Agent 应检查当前任务最匹配的技能：

| 阶段 | 技能 | 触发条件 |
|------|------|---------|
| 设计 | `brainstorming` | 任何新功能、修改行为前 |
| 计划 | `writing-plans` | 设计获批后、多步骤任务 |
| 实现 | `test-driven-development` | 编写任何实现代码前 |
| 执行 | `subagent-driven-development` | 有计划且任务独立时 |
| 审查 | `requesting-code-review` | 每任务完成后、合并前 |
| 调试 | `systematic-debugging` | 遇到 bug 或测试失败 |
| 收尾 | `finishing-a-development-branch` | 全部任务完成 |
| 验证 | `verification-before-completion` | 声称任何完成前 |

## 项目特定规则

### Rust 代码风格
- 使用 `anyhow`/`thiserror` 进行错误处理，禁止裸 `unwrap()`
- 异步上下文中的阻塞操作必须用 `spawn_blocking`
- 公共 API 必须有文档注释

### 测试要求
- 编译: `cargo build --release` 必须 exit 0
- 单元测试: `cargo test` 必须全部通过
- 集成测试: `make test-fault` 必须全部通过
- 数据清理: 每次测试后 `make clean-data`

### 版本管理
- 版本号遵循 `v0.1.X`，最小变动 +1
- `Cargo.toml` + `client/Cargo.toml` 版本号同步
- README.md 必须与代码同步更新
- 每个 tag 有完整的 release note

### 禁止事项
- 禁止在测试通过前提交
- 禁止留下测试数据
- 禁止使用 Python/Shell 写测试（用 Rust）
- 禁止不经验证声称"完成"
