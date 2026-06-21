---
name: finishing-a-development-branch
description: "当所有任务完成、测试通过、代码已审查后使用。处理合并/PR/清理决策。"
---

# 完成开发分支

## 流程

1. **最终验证**
   - `cargo build --release` — 编译通过
   - `cargo test` — 全部通过
   - `make test-fault` — 集成测试通过

2. **文档同步**
   - README.md 反映所有变更
   - 版本号已更新

3. **清理测试数据**
   - `make clean-data`

4. **提交 + 标签**
   - 最小版本号 +1 (`v0.1.X`)
   - 有意义的 commit message

5. **推送**
   - `git push origin main`
   - `git push origin v0.1.X`

## 验证清单
- [ ] 所有测试通过（单元 + 集成）
- [ ] 无编译警告
- [ ] README 已更新
- [ ] 版本号已递增
- [ ] 测试数据已清理
- [ ] 已推送到 GitHub
