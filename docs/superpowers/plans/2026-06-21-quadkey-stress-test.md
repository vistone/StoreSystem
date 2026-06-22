# QuadKey 高压测试方案 — 实现计划

**目标：** 对 StoreSystem 的 QuadKey 区域路由 + 分片存储进行全面高压测试

**架构：**
- Client → Master(:50051) → 按 quadkey[0] 路由到 4 个 Worker(50061-50064)
- Worker 按 level 分片：≤8→base, 8-18→4位前缀, ≥18→8位前缀
- 存储路径：`quad_data/{data_type}/{level}/{prefix}.{ext}`

**技术栈：** Rust + tonic(gRPC) + tokio

## 全局约束

- 不修改服务端代码，只改 client 测试代码
- 测试通过 gRPC 走 Master 路由（Client → Master → Worker）
- 测试覆盖所有 4 个区域（0/1/2/3）
- 测试覆盖所有 3 个 level 区间（≤8, 8-18, ≥18）
- 测试验证数据写入后能正确读取
- 测试验证分片文件路径正确生成
- 测试验证高压下的稳定性和零失败

---

### 任务 1: 编写全新高压测试 client

**文件：**
- 修改: `client/src/main.rs` — 全新测试主流程
- 修改: `client/src/grpc_client.rs` — 新增测试方法

**测试覆盖：**

| # | 测试项 | 说明 |
|---|--------|------|
| 1 | 4区域路由验证 | 向 region 0/1/2/3 各写 100 条，验证读写成功 |
| 2 | 3级分片验证 | level=5(≤8→base), level=12(8-18→4位), level=20(≥18→8位) |
| 3 | 区域高压写入 | 50并发×1000条×1KB → 每个区域独立测试 |
| 4 | 全区域混合高压 | 50并发×4000条×1KB → 均匀分布到 4 个区域 |
| 5 | 大文件跨区域 | 1MB 文件写入 4 个区域各 100 条 |
| 6 | 混合读写 | 20读+20写并发，跨区域随机 |
| 7 | 长时间稳定性 | 30秒持续写入，10并发 |
| 8 | 分片文件验证 | 检查 `quad_data/` 下生成的文件路径是否正确 |

- [ ] **Step 1: 编写 `quadkey_routing_stress_test()` — 4区域路由验证**
  对 region 0/1/2/3 各写 100 条，然后全部读取验证
  预期: 0 失败

- [ ] **Step 2: 编写 `level_sharding_test()` — 3级分片验证**
  level=5(→base), level=12(→4位前缀), level=20(→8位前缀)
  各写 50 条，验证读写成功

- [ ] **Step 3: 编写 `region_stress_put()` — 单区域高压写入**
  50并发×1000条×1KB，分别对 4 个区域测试
  报告每个区域的吞吐和延迟

- [ ] **Step 4: 编写 `cross_region_stress()` — 全区域混合高压**
  50并发×4000条×1KB，均匀分布到 4 个区域
  验证总成功率 100%

- [ ] **Step 5: 编写 `large_file_cross_region()` — 大文件跨区域**
  1MB 文件写入 4 个区域各 100 条
  验证读写成功

- [ ] **Step 6: 编写 `cross_region_mixed()` — 跨区域混合读写**
  20读+20写并发，随机选择区域
  验证 0 失败

- [ ] **Step 7: 编写 `stability_test_30s()` — 长时间稳定性**
  10并发×30秒，随机区域
  每秒报告吞吐

- [ ] **Step 8: 编译运行验证**
  `cd client && cargo run --release`
  预期: 所有测试 0 失败

- [ ] **Step 9: 验证分片文件路径**
  `find quad_data/ -type f` 检查文件结构
  预期: 看到 base.kv, 12/xxxx.kv, 20/xxxxxxxx.kv 等
