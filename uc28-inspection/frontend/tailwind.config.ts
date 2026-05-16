import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        uc: {
          "bg-dark": "#0F2027",
          "bg-panel": "#164E5E",
          panel: "#F8FAFC",
          primary: "#134E5E",
          "primary-2": "#2C7A7B",
          accent: "#10B981",
          "accent-lt": "#6EE7B7",
          "accent-50": "#ECFDF5",
          alert: "#F59E0B",
          "alert-50": "#FEF3C7",
          danger: "#DC2626",
          "danger-50": "#FEE2E2",
          "text-dark": "#0F172A",
          "text-body": "#334155",
          "text-mute": "#64748B",
          border: "#CBD5E1",
        },
      },
      fontFamily: {
        sans: ["Inter", "sans-serif"],
        mono: ["JetBrains Mono", "monospace"],
      },
    },
  },
  plugins: [],
};

export default config;
