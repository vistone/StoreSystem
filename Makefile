.PHONY: all build build-server build-client build-guardian clean clean-data clean-all test test-fault ci

all: build

# ============================================================
# CI 门禁（与 GitHub Actions 一致，提交前必须通过）
# ============================================================
ci:
	@echo "=== 1/4 cargo fmt --all -- --check ==="
	cargo fmt --all -- --check
	@echo "=== 2/4 cargo clippy --all -- -D warnings ==="
	cargo clippy --all -- -D warnings
	@echo "=== 3/4 cargo build --release --all ==="
	cargo build --release --all
	@echo "=== 4/4 cargo test --all ==="
	cargo test --all
	@echo ""
	@echo "✅ CI 门禁全部通过"

# 编译 server、client 和 guardian，并将可执行文件输出到 bin/ 目录
build: build-server build-client build-guardian

build-server:
	/home/stone/.cargo/bin/cargo build --release
	cp target/release/store_system bin/store_system
	@echo "✅ server 已编译: bin/store_system"

build-client:
	cd client && /home/stone/.cargo/bin/cargo build --release --bin store_client --bin fault_test
	cp client/target/release/store_client bin/store_client
	cp client/target/release/fault_test bin/fault_test
	@echo "✅ client 已编译: bin/store_client, bin/fault_test"

build-guardian:
	/home/stone/.cargo/bin/cargo build --release -p store_guardian
	cp target/release/store_guardian bin/store_guardian
	@echo "✅ guardian 已编译: bin/store_guardian"

# ============================================================
# 测试（自动启动集群 + 运行测试 + 清理）
# ============================================================

# 性能测试
test: build
	@echo "=== 启动集群 ==="
	mkdir -p master_data worker_data/worker-1 worker_data/worker-2
	./bin/store_system --config master.yaml > /dev/null 2>&1 &
	@sleep 3
	./bin/store_system --config worker.yaml > /dev/null 2>&1 &
	./bin/store_system --config worker2.yaml > /dev/null 2>&1 &
	@sleep 5
	@echo "=== 运行性能测试 ==="
	cd client && ../bin/store_client
	@echo "=== 清理 ==="
	-pgrep -f "/bin/store_system" | xargs -r kill 2>/dev/null; true
	@sleep 1
	rm -rf master_data worker_data data shard_data
	@echo "✅ 测试完成，数据已清理"

# 故障恢复测试
test-fault: build
	@echo "=== 启动集群 ==="
	mkdir -p master_data worker_data/worker-1 worker_data/worker-2
	./bin/store_system --config master.yaml > /dev/null 2>&1 &
	@sleep 3
	./bin/store_system --config worker.yaml > /dev/null 2>&1 &
	./bin/store_system --config worker2.yaml > /dev/null 2>&1 &
	@sleep 5
	@echo "=== 运行故障恢复测试 ==="
	./bin/fault_test
	@echo "=== 清理 ==="
	-pgrep -f "/bin/store_system" | xargs -r kill 2>/dev/null; true
	@sleep 1
	rm -rf master_data worker_data data shard_data
	@echo "✅ 故障测试完成，数据已清理"

# ============================================================
# 清理
# ============================================================

# 清理编译产物
clean:
	rm -rf bin/store_system bin/store_client bin/fault_test bin/store_guardian
	@echo "✅ bin/ 目录已清理"

# 清理所有测试数据（数据库文件 + 临时日志）
clean-data:
	rm -rf master_data worker_data data shard_data quad_data data_sa
	-pgrep -f "/bin/store_system" | xargs -r kill 2>/dev/null; true
	@echo "✅ 测试数据已清理"

# 深度清理（数据 + 编译缓存）
clean-all: clean-data
	rm -rf bin/
	/home/stone/.cargo/bin/cargo clean
	cd client && /home/stone/.cargo/bin/cargo clean
	@echo "✅ 深度清理完成"
