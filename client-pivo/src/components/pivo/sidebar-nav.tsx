"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

type SidebarNavProps = {
  ownerId: string;
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

export function SidebarNav({ ownerId }: SidebarNavProps) {
  const pathname = usePathname();
  const items = getItems(ownerId);

  return (
    <aside className="flex h-full flex-col rounded-3xl border border-slate-200 bg-white p-4 shadow-sm">
      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">PIVO</p>
      <h2 className="mt-2 text-xl font-bold text-slate-900">Control Panel</h2>
      <p className="mt-1 text-sm leading-relaxed text-slate-600">From data to daily decisions.</p>

      <nav className="mt-4 space-y-2">
        {items.map((item) => {
          const active = pathname === item.href;

          return (
            <Link
              key={item.href}
              href={item.href}
              className={`block rounded-xl border px-3 py-2 text-sm font-medium transition ${navClass(active)}`}
            >
              {item.label}
            </Link>
          );
        })}
      </nav>

      <div className="mt-auto rounded-xl bg-[var(--pivo-primary)] p-3 text-xs text-slate-600">
        Owner route
        <p className="mt-1 font-mono text-[11px] text-slate-700">/u/{ownerId}</p>
      </div>
    </aside>
  );
}
