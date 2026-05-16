/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
  },
  webpack: (config, { dev }) => {
    if (dev) {
      // Webpack 13.2 only accepts RegExp|string|string[] for ignored (no functions).
      // Build a regex that ignores node_modules and anything outside $HOME,
      // so Watchpack never tries to scan /data, /proc, /sys etc. on Android.
      const home = (process.env.HOME || process.cwd()).replace(
        /[.*+?^${}()|[\]\\]/g, "\\$&"
      );
      config.watchOptions = {
        poll: 2000,
        aggregateTimeout: 500,
        ignored: new RegExp(`(node_modules|^(?!${home}))`),
      };
    }
    return config;
  },
};

export default nextConfig;
