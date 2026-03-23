import Link from "next/link";

type OwnerNavProps = {
  ownerId: string;
  active: "dashboard" | "history";
};

function tabClass(isActive: boolean): string {
  if (isActive) {
    return "bg-[var(--pivo-navy)] text-white shadow-sm";
  }

  return "bg-white text-slate-700 hover:bg-[var(--pivo-primary)]";
}

export function OwnerNav({ ownerId, active }: OwnerNavProps) {
  return (
    <nav className="inline-flex rounded-2xl border border-slate-200 p-1 shadow-sm">
      <Link
        href={`/u/${ownerId}`}
        className={`rounded-xl px-3 py-2 text-sm font-medium transition ${tabClass(active === "dashboard")}`}
      >
        Dashboard
      </Link>
      <Link
        href={`/u/${ownerId}/history`}
        className={`rounded-xl px-3 py-2 text-sm font-medium transition ${tabClass(active === "history")}`}
      >
        Sales History
      </Link>
    </nav>
  );
}
