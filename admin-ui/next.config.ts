import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // 使用 webpack 替代 Turbopack（Turbopack 的 HMR WebSocket 有兼容性问题）
  turbopack: {},
  // 允许来自 127.0.0.1 的跨域 HMR WebSocket 请求
  // 解决 "Blocked cross-origin request to /_next/webpack-hmr from 127.0.0.1" 错误
  // 注意：必须用顶层的 allowedDevOrigins，experimental.serverActions.allowedOrigins
  // 只对 Server Actions 生效，不会放行 HMR WebSocket
  allowedDevOrigins: ["127.0.0.1", "localhost"],
  devIndicators: false,
  experimental: {
    serverActions: {
      allowedOrigins: ["127.0.0.1", "localhost"],
    },
  },
};

export default nextConfig;

