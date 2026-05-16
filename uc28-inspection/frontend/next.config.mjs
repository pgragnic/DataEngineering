/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
  },
  webpack: (config, { dev }) => {
    if (dev) {
      const projectDir = process.cwd();
      // Restrict watcher to project dir only — prevents EACCES flood on Android
      // that crashes Watchpack with unhandled errors after compilation
      config.watchOptions = {
        poll: 2000,
        aggregateTimeout: 500,
        ignored: (absPath) =>
          absPath.includes("node_modules") || !absPath.startsWith(projectDir),
      };
    }
    return config;
  },
};

export default nextConfig;
