use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// 磁盘健康状态
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DiskHealth {
    /// 健康
    Healthy,
    /// 警告（即将满）
    Warning,
    /// 危险（已满或即将故障）
    Critical,
    /// 未知
    Unknown,
}

/// 系统健康信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    /// 采集时间戳（Unix 秒）
    pub timestamp: i64,

    // ---- 存储 ----
    /// 已用存储字节数
    pub storage_used_bytes: u64,
    /// 总存储容量字节数
    pub storage_capacity_bytes: u64,
    /// 存储使用率（0.0 ~ 1.0）
    pub storage_usage_ratio: f64,
    /// 磁盘健康状态
    pub disk_health: DiskHealth,

    // ---- 内存 ----
    /// 已用内存字节数
    pub memory_used_bytes: u64,
    /// 总内存字节数
    pub memory_total_bytes: u64,
    /// 内存使用率（0.0 ~ 1.0）
    pub memory_usage_ratio: f64,

    // ---- CPU ----
    /// CPU 使用率（0.0 ~ 1.0）
    pub cpu_usage_ratio: f64,
    /// CPU 核心数
    pub cpu_cores: u32,
}

impl HealthInfo {
    /// 采集当前系统健康信息
    ///
    /// # 参数
    /// - `data_dir`: 数据目录路径，用于检测磁盘空间
    pub fn collect(data_dir: impl AsRef<Path>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let (storage_used, storage_capacity) = Self::get_disk_usage(&data_dir);
        let storage_usage_ratio = if storage_capacity > 0 {
            storage_used as f64 / storage_capacity as f64
        } else {
            0.0
        };

        let disk_health = if storage_usage_ratio >= 0.95 {
            DiskHealth::Critical
        } else if storage_usage_ratio >= 0.85 {
            DiskHealth::Warning
        } else {
            DiskHealth::Healthy
        };

        let (memory_used, memory_total) = Self::get_memory_usage();
        let memory_usage_ratio = if memory_total > 0 {
            memory_used as f64 / memory_total as f64
        } else {
            0.0
        };

        let cpu_usage_ratio = Self::get_cpu_usage();
        let cpu_cores = num_cpus::get() as u32;

        Self {
            timestamp: now,
            storage_used_bytes: storage_used,
            storage_capacity_bytes: storage_capacity,
            storage_usage_ratio,
            disk_health,
            memory_used_bytes: memory_used,
            memory_total_bytes: memory_total,
            memory_usage_ratio,
            cpu_usage_ratio,
            cpu_cores,
        }
    }

    /// 获取磁盘使用情况（通过 `statvfs` 系统调用）
    fn get_disk_usage(data_dir: impl AsRef<Path>) -> (u64, u64) {
        #[cfg(target_os = "linux")]
        {
            use std::ffi::CString;
            use std::mem::MaybeUninit;

            let path = data_dir.as_ref();
            // 确保目录存在
            let _ = std::fs::create_dir_all(path);

            let c_path = match CString::new(path.to_string_lossy().as_bytes()) {
                Ok(p) => p,
                Err(_) => return (0, 0),
            };

            unsafe {
                let mut stat: libc::statvfs = MaybeUninit::zeroed().assume_init();
                if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                    let total = stat.f_blocks * stat.f_frsize;
                    let available = stat.f_bavail * stat.f_frsize;
                    let used = total - available;
                    return (used, total);
                }
            }
            (0, 0)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = data_dir;
            (0, 0)
        }
    }

    /// 获取内存使用情况
    fn get_memory_usage() -> (u64, u64) {
        #[cfg(target_os = "linux")]
        {
            // 从 /proc/meminfo 读取内存信息
            if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
                let mut total: u64 = 0;
                let mut available: u64 = 0;

                for line in content.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(val) = line.split_whitespace().nth(1) {
                            total = val.parse::<u64>().unwrap_or(0) * 1024; // kB -> bytes
                        }
                    } else if line.starts_with("MemAvailable:") {
                        if let Some(val) = line.split_whitespace().nth(1) {
                            available = val.parse::<u64>().unwrap_or(0) * 1024;
                        }
                    }
                }

                let used = total.saturating_sub(available);
                return (used, total);
            }
            (0, 0)
        }

        #[cfg(not(target_os = "linux"))]
        {
            (0, 0)
        }
    }

    /// 获取 CPU 使用率（通过读取 /proc/stat 计算）
    fn get_cpu_usage() -> f64 {
        #[cfg(target_os = "linux")]
        {
            // 读取两次 /proc/stat，间隔 100ms，计算 CPU 使用率
            let cpu_times1 = Self::read_cpu_times();
            if cpu_times1.is_empty() {
                return 0.0;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
            let cpu_times2 = Self::read_cpu_times();
            if cpu_times2.is_empty() {
                return 0.0;
            }

            let idle1 = cpu_times1[3]; // idle
            let total1: u64 = cpu_times1.iter().sum();

            let idle2 = cpu_times2[3];
            let total2: u64 = cpu_times2.iter().sum();

            let total_delta = total2.saturating_sub(total1);
            let idle_delta = idle2.saturating_sub(idle1);

            if total_delta > 0 {
                1.0 - (idle_delta as f64 / total_delta as f64)
            } else {
                0.0
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            0.0
        }
    }

    /// 读取 /proc/stat 中的 CPU 时间
    #[cfg(target_os = "linux")]
    fn read_cpu_times() -> Vec<u64> {
        if let Ok(content) = std::fs::read_to_string("/proc/stat") {
            if let Some(line) = content.lines().next() {
                // 格式: "cpu  user nice system idle iowait irq softirq steal guest guest_nice"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 5 && parts[0] == "cpu" {
                    return parts[1..]
                        .iter()
                        .filter_map(|s| s.parse::<u64>().ok())
                        .collect();
                }
            }
        }
        Vec::new()
    }
}

/// 检查存储是否超过阈值
///
/// # 返回值
/// - `Ok(())`: 存储空间充足
/// - `Err(String)`: 存储空间不足，返回错误信息
pub fn check_storage_capacity(
    health: &HealthInfo,
    storage_threshold_ratio: f64,
) -> Result<(), String> {
    if health.storage_usage_ratio >= storage_threshold_ratio {
        return Err(format!(
            "存储空间不足: 已用 {:.1}% (阈值: {:.1}%), 已用 {}/{}",
            health.storage_usage_ratio * 100.0,
            storage_threshold_ratio * 100.0,
            format_bytes(health.storage_used_bytes),
            format_bytes(health.storage_capacity_bytes),
        ));
    }

    if health.disk_health == DiskHealth::Critical {
        return Err(format!(
            "磁盘健康状态严重: 使用率 {:.1}%",
            health.storage_usage_ratio * 100.0,
        ));
    }

    Ok(())
}

/// 格式化字节数为人类可读形式
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_storage_capacity_ok() {
        let health = HealthInfo {
            timestamp: 0,
            storage_used_bytes: 50 * 1024 * 1024 * 1024, // 50GB
            storage_capacity_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            storage_usage_ratio: 0.5,
            disk_health: DiskHealth::Healthy,
            memory_used_bytes: 0,
            memory_total_bytes: 0,
            memory_usage_ratio: 0.0,
            cpu_usage_ratio: 0.0,
            cpu_cores: 0,
        };

        assert!(check_storage_capacity(&health, 0.9).is_ok());
    }

    #[test]
    fn test_check_storage_capacity_exceeded() {
        let health = HealthInfo {
            timestamp: 0,
            storage_used_bytes: 95 * 1024 * 1024 * 1024,
            storage_capacity_bytes: 100 * 1024 * 1024 * 1024,
            storage_usage_ratio: 0.95,
            disk_health: DiskHealth::Critical,
            memory_used_bytes: 0,
            memory_total_bytes: 0,
            memory_usage_ratio: 0.0,
            cpu_usage_ratio: 0.0,
            cpu_cores: 0,
        };

        assert!(check_storage_capacity(&health, 0.9).is_err());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0.00 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }
}
