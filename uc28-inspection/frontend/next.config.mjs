/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000",
  },
  webpack: (config, { dev }) => {
    if (dev) {
      // On Android/Termux inotify causes EACCES spam → use polling instead
      config.watchOptions = {
        poll: 2000,
        aggregateTimeout: 500,
        ignored: /node_modules/,
      };
    }
    return config;
  },
};

export default nextConfig;
