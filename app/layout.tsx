import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "RKMT Plan Runtime",
  description: "Prototype planner-approval-executor workflow with OpenAI Agent SDK",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
