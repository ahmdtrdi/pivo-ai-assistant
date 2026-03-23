"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";

type SidebarNavProps = {
  ownerId: string;
  onNavigate?: () => void;
};

type NavItem = {
  label: string;
  href: string;
};

function getItems(ownerId: string): NavItem[] {
  return [
    { label: "Dashboard", href: `/u/${ownerId}` },
    { label: "Simulator", href: `/u/${ownerId}/simulator` },
    { label: "History", href: `/u/${ownerId}/history` },
    { label: "Settings", href: `/u/${ownerId}/settings` },
  ];
}

function navClass(active: boolean): string {
  if (active) {
    return "border-[var(--pivo-blue)] bg-[var(--pivo-navy)] text-white";
  }

  return "border-transparent bg-white text-slate-700 hover:border-[var(--pivo-blue)]/35 hover:bg-[var(--pivo-primary)]";
}

export function SidebarNav({ ownerId, onNavigate }: SidebarNavProps) {
  const pathname = usePathname();
  const items = getItems(ownerId);

  return (
    <aside className="flex h-full flex-col rounded-3xl border border-slate-200 bg-white p-3 shadow-sm sm:p-4">
      <div className="rounded-2xl bg-[var(--pivo-primary)]/70 px-2 py-3">
        <Image
          src="/dashboard-pivo.svg"
          alt="PIVO dashboard"
          width={220}
          height={56}
          priority
          className="h-auto w-full max-w-[190px]"
        />
      </div>

      <nav className="mt-5 space-y-2">
        {items.map((item) => {
          const active = pathname === item.href;

          return (
            <Link
              key={item.href}
              href={item.href}
              onClick={onNavigate}
              className={`block rounded-xl border px-3 py-2 text-sm font-medium transition ${navClass(active)}`}
            >
              {item.label}
            </Link>
          );
        })}
      </nav>

      <p className="mt-auto px-2 pt-6 text-xs text-slate-500">Copyright 2026</p>
    </aside>
  );
}
