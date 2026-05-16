import type { Metadata } from "next";
import "./globals.css";
import { QueryProvider } from "@/lib/query-provider";

export const metadata: Metadata = {
  title: "BV·Inspect — Inspection Augmentée",
  description: "Copilote IA pour les inspecteurs Bureau Veritas",
  manifest: "/manifest.webmanifest",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body className="font-sans bg-uc-panel text-uc-text-dark antialiased">
        <QueryProvider>{children}</QueryProvider>
      </body>
    </html>
  );
}
