use std::time::Duration;

/// 探针结果
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ProbeResult {
    Running,
    Degraded(String),
    Timeout,
    ConnectionRefused,
    ProcessGone,
}

/// 对 Master 执行深度探针（StoreService.Put + Get 往返）
pub async fn probe_master(grpc_addr: &str, timeout_secs: u64) -> ProbeResult {
    use crate::proto;

    let endpoint = match tonic::transport::Endpoint::from_shared(format!("http://{}", grpc_addr)) {
        Ok(e) => e
            .timeout(Duration::from_secs(timeout_secs))
            .connect_timeout(Duration::from_secs(3)),
        Err(_) => return ProbeResult::Degraded("invalid address".into()),
    };

    let mut client = match proto::store_service_client::StoreServiceClient::connect(endpoint).await
    {
        Ok(c) => c,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("refused") || msg.contains("Connection refused") {
                return ProbeResult::ConnectionRefused;
            }
            return ProbeResult::Degraded(msg);
        }
    };

    let now = chrono::Utc::now().timestamp_millis().to_string();
    let value = now.as_bytes().to_vec();
    let health_key = "__health__";

    // Put
    let put_req = tonic::Request::new(proto::PutRequest {
        key: health_key.to_string(),
        value: value.clone(),
        quadkey: "0".to_string(),
        level: 0,
        ..Default::default()
    });

    if let Err(e) = client.put(put_req).await {
        return ProbeResult::Degraded(format!("put failed: {}", e));
    }

    // Get
    let get_req = tonic::Request::new(proto::GetRequest {
        key: health_key.to_string(),
        quadkey: "0".to_string(),
        level: 0,
        ..Default::default()
    });

    match client.get(get_req).await {
        Ok(resp) => {
            let got = resp.into_inner().value;
            if got == value {
                ProbeResult::Running
            } else {
                ProbeResult::Degraded("value mismatch".into())
            }
        }
        Err(e) => ProbeResult::Degraded(format!("get failed: {}", e)),
    }
}

/// 对 Worker 执行深度探针（WorkerService.Put + Get 往返）
pub async fn probe_worker(grpc_addr: &str, timeout_secs: u64) -> ProbeResult {
    use crate::proto;

    let endpoint = match tonic::transport::Endpoint::from_shared(format!("http://{}", grpc_addr)) {
        Ok(e) => e
            .timeout(Duration::from_secs(timeout_secs))
            .connect_timeout(Duration::from_secs(3)),
        Err(_) => return ProbeResult::Degraded("invalid address".into()),
    };

    let mut client =
        match proto::worker_service_client::WorkerServiceClient::connect(endpoint).await {
            Ok(c) => c,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("refused") || msg.contains("Connection refused") {
                    return ProbeResult::ConnectionRefused;
                }
                return ProbeResult::Degraded(msg);
            }
        };

    let now = chrono::Utc::now().timestamp_millis().to_string();
    let value = now.as_bytes().to_vec();
    let health_key = "__health__";

    // Put
    let put_req = tonic::Request::new(proto::PutRequest {
        key: health_key.to_string(),
        value: value.clone(),
        quadkey: "0".to_string(),
        level: 0,
        ..Default::default()
    });

    if let Err(e) = client.put(put_req).await {
        return ProbeResult::Degraded(format!("put failed: {}", e));
    }

    // Get
    let get_req = tonic::Request::new(proto::GetRequest {
        key: health_key.to_string(),
        quadkey: "0".to_string(),
        level: 0,
        ..Default::default()
    });

    match client.get(get_req).await {
        Ok(resp) => {
            let got = resp.into_inner().value;
            if got == value {
                ProbeResult::Running
            } else {
                ProbeResult::Degraded("value mismatch".into())
            }
        }
        Err(e) => ProbeResult::Degraded(format!("get failed: {}", e)),
    }
}
