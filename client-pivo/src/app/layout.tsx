import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "PIVO | From Data to Daily Decisions",
  description:
    "PIVO helps MSME owners turn daily sales records into practical production and profit decisions.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="id" className="h-full antialiased">
      <body className="min-h-full bg-[var(--pivo-primary)] text-slate-900">{children}</body>
    </html>
  );
}
