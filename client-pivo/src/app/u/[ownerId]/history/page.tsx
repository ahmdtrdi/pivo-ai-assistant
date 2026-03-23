import { OwnerNav } from "@/components/pivo/owner-nav";
import { SalesHistoryCharts } from "@/components/pivo/sales-history-charts";
import { getOwnerPayload } from "@/lib/payload.server";

export const revalidate = 0;

type OwnerHistoryPageProps = {
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerHistoryPage({ params }: OwnerHistoryPageProps) {
  const { ownerId } = await params;
  const payload = await getOwnerPayload(ownerId);

  return (
    <main className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
      <section className="rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">PIVO Page 3</p>
        <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">Sales History</h1>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-700 sm:text-base">
          Lihat pola penjualan 30 hari terakhir dan bandingkan gross profit mingguan untuk keputusan operasional yang lebih presisi.
        </p>
        <div className="mt-4">
          <OwnerNav ownerId={ownerId} active="history" />
        </div>
      </section>

      <SalesHistoryCharts rows={payload.history_30d ?? []} />
    </main>
  );
}
