.PHONY: all build build-server build-client clean

all: build

# 编译 server 和 client，并将可执行文件输出到 bin/ 目录
build: build-server build-client

build-server:
	cargo build --release
	cp target/release/store_system bin/store_system
	@echo "✅ server 已编译: bin/store_system"

build-client:
	cd client && cargo build --release
	cp client/target/release/store_client bin/store_client
	@echo "✅ client 已编译: bin/store_client"

clean:
	rm -rf bin/store_system bin/store_client
	@echo "✅ bin/ 目录已清理"
