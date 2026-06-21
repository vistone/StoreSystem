use store_system::grpc::proto::master_service_client::MasterServiceClient;
use store_system::grpc::proto::store_service_client::StoreServiceClient;
use store_system::grpc::proto::{GetRequest, GetRouteRequest, PutRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let master_addr = "http://localhost:50051";
    let mut master = MasterServiceClient::connect(master_addr)
        .await?
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);
    let mut store = StoreServiceClient::connect(master_addr)
        .await?
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);

    println!("=== 路由稳定性测试 ===");
    // 测试同一个 key 多次路由是否返回相同 worker
    for key in &["test_key_1", "grpc_read_1024_12", "hello"] {
        let mut routes = Vec::new();
        for _ in 0..3 {
            let resp = master
                .get_route(GetRouteRequest {
                    key: key.to_string(),
                })
                .await?;
            routes.push(resp.into_inner().worker_id);
        }
        let stable = routes.windows(2).all(|w| w[0] == w[1]);
        println!("  {} -> {:?} 稳定: {}", key, routes, stable);
    }

    println!("\n=== 写入并立即读取测试 ===");
    for i in 0..10 {
        let key = format!("test_rw_{}", i);
        let value = format!("value_{}", i).into_bytes();

        // 查询路由
        let route = master
            .get_route(GetRouteRequest { key: key.clone() })
            .await?;
        let worker_id = route.into_inner().worker_id;

        // 写入
        let put_req = PutRequest {
            key: key.clone(),
            value: value.clone(),
            content_type: "text/plain".to_string(),
            tags: String::new(),
        };
        store.put(put_req).await?;

        // 等待刷盘
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 读取
        let get_resp = store.get(GetRequest { key: key.clone() }).await;
        match get_resp {
            Ok(resp) => {
                let got = resp.into_inner().value;
                let ok = got == value;
                println!(
                    "  {} (worker={}): 写入={}字节 读取={}字节 匹配={}",
                    key,
                    worker_id,
                    value.len(),
                    got.len(),
                    ok
                );
            }
            Err(e) => {
                println!(
                    "  {} (worker={}): 写入成功但读取失败: {}",
                    key, worker_id, e
                );
            }
        }
    }

    println!("\n=== 批量写入后读取测试（模拟 client 测试场景）===");
    let keys: Vec<String> = (0..20).map(|i| format!("grpc_read_1024_{}", i)).collect();

    // 批量写入
    for key in &keys {
        let route = master
            .get_route(GetRouteRequest { key: key.clone() })
            .await?;
        let wid = route.into_inner().worker_id;
        let put_req = PutRequest {
            key: key.clone(),
            value: vec![b'x'; 1024],
            content_type: "text/plain".to_string(),
            tags: String::new(),
        };
        store.put(put_req).await?;
        print!("  写入 {} -> worker-{}\r", key, wid);
    }
    println!();

    // 等待刷盘
    println!("等待刷盘 100ms...");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 逐个读取
    let mut ok_count = 0;
    let mut fail_count = 0;
    for key in &keys {
        let route = master
            .get_route(GetRouteRequest { key: key.clone() })
            .await?;
        let wid = route.into_inner().worker_id;
        let get_resp = store.get(GetRequest { key: key.clone() }).await;
        match get_resp {
            Ok(_) => {
                ok_count += 1;
            }
            Err(e) => {
                fail_count += 1;
                println!("  读取失败: {} (worker={}) - {}", key, wid, e.message());
            }
        }
    }
    println!("结果: 成功 {} 条, 失败 {} 条", ok_count, fail_count);

    Ok(())
}
