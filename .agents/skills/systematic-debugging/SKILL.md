---
name: systematic-debugging
description: "当遇到 bug、测试失败或意外行为时使用。四阶段根本原因分析过程。"
---

# 系统性调试

## 四阶段过程

### 阶段 1: 复现
- 编写最小复现案例
- 记录确切输入、预期输出、实际输出
- 确认每次都能复现

### 阶段 2: 隔离
- 二分法缩小范围
- 每次只改变一个变量
- 找到最小触发条件

### 阶段 3: 理解
- 阅读相关代码路径
- 添加日志（eprintln!/tracing）追踪状态
- 形成假设 → 测试假设

### 阶段 4: 修复 + 预防
- 实施修复
- 编写回归测试
- 验证修复通过 RED-GREEN 循环
- 检查是否有同类 bug

## Rust 调试命令
```
cargo test -- --nocapture    # 显示测试输出
RUST_BACKTRACE=1 cargo run   # 获取堆栈跟踪
cargo test test_name -- --test-threads=1  # 单线程调试
```
