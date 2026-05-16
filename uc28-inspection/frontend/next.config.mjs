/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
  },
  webpack: (config, { dev }) => {
    if (dev) {
      // On Android/Termux the watcher lacks permission to scan system dirs
      config.watchOptions = {
        ...config.watchOptions,
        ignored: ["**/node_modules/**", "/data/**", "/proc/**", "/sys/**"],
      };
    }
    return config;
  },
};

export default nextConfig;
