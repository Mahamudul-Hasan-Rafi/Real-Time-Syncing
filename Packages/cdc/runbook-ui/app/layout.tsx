import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "CDC Runbook",
  description: "Oracle to Kafka CDC database setup runbook",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
