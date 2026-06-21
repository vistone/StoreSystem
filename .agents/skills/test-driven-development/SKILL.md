---
name: test-driven-development
description: "在编写任何实现代码之前，实现功能或修复 bug 时必须使用。遵循 RED-GREEN-REFACTOR 循环。"
---

# 测试驱动开发 (TDD)

## 铁律

```
没有先写失败测试，就没有生产代码
```

## RED-GREEN-REFACTOR

### RED — 编写失败测试

```rust
#[test]
fn test_specific_behavior() {
    let result = function(input);
    assert_eq!(result, expected);
}
```

**要求：** 一个行为，清晰命名，真实代码

### 验证 RED — 必须看到失败

```bash
cargo test test_specific_behavior
# 预期: FAIL，因为功能尚未实现
```

### GREEN — 最小代码

编写最简单能通过测试的代码。不要添加功能、不要重构其他代码。

### 验证 GREEN — 必须看到通过

```bash
cargo test
# 预期: 全部通过
```

### REFACTOR — 清理

仅在绿灯后：消除重复、改善命名、提取辅助函数。

## 关键反模式

| 借口 | 现实 |
|------|------|
| "太简单不需要测试" | 简单代码也会出错 |
| "写完再加测试" | 立即通过的测试不证明任何事 |
| "手动测过了" | 临时测试 ≠ 系统性测试 |
| "TDD 太教条，实际点" | TDD 就是实际的——提前发现 bug |
