"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Home, Users, Star, TrendingUp } from "lucide-react";

const navItems = [
  { href: "/", label: "Home", icon: Home },
  { href: "/enrollment", label: "Enrollment", icon: Users },
  { href: "/stars", label: "Stars", icon: Star },
  { href: "/risk-scores", label: "Risk Scores", icon: TrendingUp },
];

export function Navigation() {
  const pathname = usePathname();

  return (
    <nav className="bg-white border-b shadow-sm">
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex space-x-8">
          {navItems.map((item) => {
            const isActive = pathname === item.href ||
              (item.href !== "/" && pathname.startsWith(item.href));
            const Icon = item.icon;

            return (
              <Link
                key={item.href}
                href={item.href}
                className={`flex items-center gap-2 py-4 px-2 border-b-2 transition-colors ${
                  isActive
                    ? "border-blue-600 text-blue-600"
                    : "border-transparent text-gray-600 hover:text-blue-600 hover:border-blue-300"
                }`}
              >
                <Icon className="w-5 h-5" />
                <span className="font-medium">{item.label}</span>
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
}
