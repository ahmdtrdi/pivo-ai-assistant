"use client";

import { useEffect, useState } from "react";

import { SidebarNav } from "@/components/pivo/sidebar-nav";

type OwnerShellProps = {
  ownerId: string;
  children: React.ReactNode;
};

function gridClass(isDesktopPanelOpen: boolean): string {
  if (!isDesktopPanelOpen) {
    return "grid gap-3 md:gap-4";
  }

  return "grid gap-3 md:grid-cols-[250px_minmax(0,1fr)] md:gap-4 xl:grid-cols-[260px_minmax(0,1fr)]";
}

export function OwnerShell({ ownerId, children }: OwnerShellProps) {
  const [isDesktopPanelOpen, setDesktopPanelOpen] = useState(true);
  const [isMobilePanelOpen, setMobilePanelOpen] = useState(false);

  useEffect(() => {
    if (!isMobilePanelOpen) {
      document.body.style.removeProperty("overflow");
      return;
    }

    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.removeProperty("overflow");
    };
  }, [isMobilePanelOpen]);

  return (
    <>
      <div className={gridClass(isDesktopPanelOpen)}>
        {isDesktopPanelOpen ? (
          <div className="hidden md:block md:sticky md:top-3 md:h-[calc(100vh-1.5rem)]">
            <SidebarNav ownerId={ownerId} />
          </div>
        ) : null}

        <section className="min-w-0">
          <div className="mb-3 flex items-center gap-2">
            <button
              type="button"
              onClick={() => setMobilePanelOpen(true)}
              className="inline-flex rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm md:hidden"
            >
              Open panel
            </button>

            <button
              type="button"
              onClick={() => setDesktopPanelOpen((prev) => !prev)}
              className="hidden rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm md:inline-flex"
            >
              {isDesktopPanelOpen ? "Close panel" : "Open panel"}
            </button>
          </div>

          {children}
        </section>
      </div>

      <div className={`fixed inset-0 z-40 bg-slate-950/30 transition md:hidden ${isMobilePanelOpen ? "opacity-100" : "pointer-events-none opacity-0"}`} onClick={() => setMobilePanelOpen(false)} />

      <aside
        className={`fixed inset-y-0 left-0 z-50 w-[84vw] max-w-[300px] transform p-2 transition-transform md:hidden ${
          isMobilePanelOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="mb-2 flex justify-end">
          <button
            type="button"
            onClick={() => setMobilePanelOpen(false)}
            className="rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm"
          >
            Close
          </button>
        </div>

        <div className="h-[calc(100%-3rem)]">
          <SidebarNav
            ownerId={ownerId}
            onNavigate={() => {
              setMobilePanelOpen(false);
            }}
          />
        </div>
      </aside>
    </>
  );
}
