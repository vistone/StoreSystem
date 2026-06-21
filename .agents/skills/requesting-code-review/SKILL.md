---
name: requesting-code-review
description: "在完成任务、实现主要功能或合并前验证工作是否符合要求时使用。"
---

# 请求代码审查

## 何时请求审查

**强制：**
- 子代理驱动开发中每个任务完成后
- 完成主要功能后
- 合并到 main 前

## 流程

1. 获取 git SHA：`BASE_SHA=$(git rev-parse HEAD~1)`，`HEAD_SHA=$(git rev-parse HEAD)`
2. 派生子代理审查：用 `git diff BASE_SHA..HEAD_SHA` 给审查者完整的 diff
3. 审查者检查：
   - **规格合规**：是否符合计划/需求
   - **代码质量**：Rust 最佳实践、错误处理、无 unwrap、合理使用 Result
   - **测试覆盖**：关键路径有测试

4. 对反馈采取行动：
   - **Critical**：立即修复
   - **Important**：继续前修复
   - **Minor**：记录，后续处理
