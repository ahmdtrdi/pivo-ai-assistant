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
    <section className="space-y-6">
      <section className="rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">History</p>
        <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">Sales History</h1>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-700 sm:text-base">
          Review the last 30 days of sales trend and compare weekly gross profit to support better daily decisions.
        </p>
      </section>

      <SalesHistoryCharts rows={payload.history_30d ?? []} />
    </section>
  );
}
