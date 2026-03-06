import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Providers } from "./providers";
import { Navigation } from "@/components/navigation";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "MA Intelligence Platform",
  description: "Medicare Advantage Data Intelligence Platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased bg-gray-50 h-full`}
      >
        <Providers>
          <div className="h-full flex flex-col">
            <header className="bg-blue-900 text-white shadow-lg flex-shrink-0">
              <div className="max-w-7xl mx-auto px-4 py-4">
                <h1 className="text-2xl font-bold">MA Intelligence Platform</h1>
              </div>
            </header>
            <Navigation />
            <main className="flex-1 overflow-auto">
              <div className="h-full">
                {children}
              </div>
            </main>
          </div>
        </Providers>
      </body>
    </html>
  );
}
