---
name: using-git-worktrees
description: "设计获批后使用。创建隔离工作区在新分支上，运行项目设置，验证测试基线。"
---

# 使用 Git Worktrees

## 流程

1. **创建隔离工作区**
   ```bash
   git worktree add -b feature/v0.1.X ../store-feature main
   ```

2. **切换到工作区**
   ```bash
   cd ../store-feature
   ```

3. **运行项目设置**
   ```bash
   cargo build --release && cargo test
   ```

4. **验证测试基线**
   - 所有测试通过
   - 当前状态干净可工作

5. **开始开发**
   - 遵循 brainstorming → writing-plans → subagent-driven-development 流程

## 完成后清理

```bash
git worktree remove ../store-feature
```
