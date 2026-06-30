.PHONY: all build build-server build-client build-guardian clean clean-data clean-all test test-fault ci

ifeq ($(OS),Windows_NT)
EXE := .exe
else
EXE :=
endif

CARGO ?= cargo
BIN_DIR := bin
STORE_SYSTEM := $(BIN_DIR)/store_system$(EXE)
STORE_CLIENT := $(BIN_DIR)/store_client$(EXE)
FAULT_TEST := $(BIN_DIR)/fault_test$(EXE)
STORE_GUARDIAN := $(BIN_DIR)/store_guardian$(EXE)

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
	mkdir -p $(BIN_DIR)
	$(CARGO) build --release
	cp target/release/store_system$(EXE) $(STORE_SYSTEM)
	@echo "✅ server 已编译: $(STORE_SYSTEM)"

build-client:
	mkdir -p $(BIN_DIR)
	cd client && $(CARGO) build --release --bin store_client --bin fault_test
	cp client/target/release/store_client$(EXE) $(STORE_CLIENT)
	cp client/target/release/fault_test$(EXE) $(FAULT_TEST)
	@echo "✅ client 已编译: $(STORE_CLIENT), $(FAULT_TEST)"

build-guardian:
	mkdir -p $(BIN_DIR)
	$(CARGO) build --release -p store_guardian
	cp target/release/store_guardian$(EXE) $(STORE_GUARDIAN)
	@echo "✅ guardian 已编译: $(STORE_GUARDIAN)"

# ============================================================
# 测试（自动启动集群 + 运行测试 + 清理）
# ============================================================

# 性能测试
test: build
	@echo "=== 启动集群 ==="
	mkdir -p master_data worker_data/worker-0 worker_data/worker-1 worker_data/worker-2 worker_data/worker-3
	$(STORE_SYSTEM) --config master.yaml > /dev/null 2>&1 &
	@sleep 3
		$(STORE_SYSTEM) --config worker-0.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-1.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-2.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-3.yaml > /dev/null 2>&1 &
		@sleep 5
		@echo "=== 运行性能测试 ==="
		cd client && ../$(STORE_CLIENT)
		@echo "=== 清理：杀掉所有 store_system 进程 ==="
		-pkill -9 -f store_system 2>/dev/null; true
		@sleep 1
		@echo "✅ 测试完成，数据已清理"

# 故障恢复测试
test-fault: build
	@echo "=== 启动集群 ==="
	mkdir -p master_data worker_data/worker-0 worker_data/worker-1 worker_data/worker-2 worker_data/worker-3
	$(STORE_SYSTEM) --config master.yaml > /dev/null 2>&1 &
	@sleep 3
		$(STORE_SYSTEM) --config worker-0.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-1.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-2.yaml > /dev/null 2>&1 &
		$(STORE_SYSTEM) --config worker-3.yaml > /dev/null 2>&1 &
		@sleep 5
		@echo "=== 运行故障恢复测试 ==="
		$(FAULT_TEST)
		@echo "=== 清理：杀掉所有 store_system 进程 ==="
		-pkill -9 -f store_system 2>/dev/null; true
		@sleep 1
		-$(MAKE) clean-data

# ============================================================
# 清理
# ============================================================

# 清理编译产物
clean:
	rm -rf $(STORE_SYSTEM) $(STORE_CLIENT) $(FAULT_TEST) $(STORE_GUARDIAN)
	@echo "✅ bin/ 目录已清理"

# 清理所有测试数据（数据库文件 + 临时日志）
clean-data:
		echo "=== 清理数据 ==="
		-rm -rf master_data worker_data data shard_data quad_data data_sa
		-pkill -9 -f store_system 2>/dev/null; true
		@sleep 1
		@echo "✅ 测试数据已清理"

# 深度清理（数据 + 编译缓存）
clean-all: clean-data
	rm -rf bin/
	$(CARGO) clean
	cd client && $(CARGO) clean
	@echo "✅ 深度清理完成"
