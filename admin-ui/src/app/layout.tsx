import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Store Admin - 分布式存储管理系统",
  description: "分布式 KV 存储集群管理界面",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN" className="h-full antialiased">
      <body className="min-h-full flex flex-col">{children}</body>
    </html>
  );
}
