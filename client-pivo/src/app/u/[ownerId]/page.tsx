import Link from "next/link";

import { AlertsPanel } from "@/components/pivo/alerts-panel";
import { ForecastCard } from "@/components/pivo/forecast-card";
import { OwnerNav } from "@/components/pivo/owner-nav";
import { SupplySimulator } from "@/components/pivo/supply-simulator";
import { TierBadge } from "@/components/pivo/tier-badge";
import { TopProfitList } from "@/components/pivo/top-profit-list";
import { getMarginAlerts, getRankGapInsights, getSimulatorOptions, getTopProfits } from "@/lib/dashboard";
import { titleCaseFromId } from "@/lib/format";
import { getOwnerPayload } from "@/lib/payload.server";

export const revalidate = 0;

type OwnerPageProps = {
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerDashboardPage({ params }: OwnerPageProps) {
  const { ownerId } = await params;
  const payload = await getOwnerPayload(ownerId);

  const profitRows = payload.profit_analysis ?? [];
  const topProfits = getTopProfits(profitRows, 5);
  const marginAlerts = getMarginAlerts(profitRows);
  const rankGapInsights = getRankGapInsights(profitRows);
  const simulatorOptions = getSimulatorOptions(payload);
  const displayOwnerName = titleCaseFromId(ownerId);

  return (
    <main className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
      <section className="overflow-hidden rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
        <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">PIVO Dashboard</p>
            <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">{displayOwnerName}</h1>
            <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-700 sm:text-base">
              PIVO from data to daily decisions. Fokus hari ini: keputusan produksi lebih yakin, lebih rapi, dan lebih cepat.
            </p>
            <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
              <span className="rounded-full bg-white px-3 py-1">Date: {payload.date}</span>
              <span className="rounded-full bg-white px-3 py-1">Owner ID: {payload.owner_id}</span>
            </div>
            <div className="mt-4">
              <OwnerNav ownerId={ownerId} active="dashboard" />
            </div>
          </div>

          <div className="w-full max-w-sm">
            <TierBadge tier={payload.confidence_tier} />
          </div>
        </div>
      </section>

      <section className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr]">
        <div className="space-y-4">
          <h2 className="text-lg font-semibold text-slate-900">Daily Production Recommendations</h2>

          {payload.forecasts.length === 0 ? (
            <div className="rounded-2xl border border-[var(--pivo-coral)]/35 bg-[var(--pivo-coral)]/10 p-4 text-sm text-[var(--pivo-coral-ink)]">
              Belum ada prediksi SKU hari ini. Cek konsistensi input data agar forecast aktif kembali.
            </div>
          ) : (
            <div className="grid gap-3 sm:grid-cols-2">
              {payload.forecasts.map((forecast) => (
                <ForecastCard key={forecast.sku} forecast={forecast} />
              ))}
            </div>
          )}
        </div>

        <div className="space-y-4">
          <TopProfitList rows={topProfits} />
          <AlertsPanel
            anomalyFlags={payload.anomaly_flags ?? []}
            marginAlerts={marginAlerts}
            rankGapInsights={rankGapInsights}
          />
        </div>
      </section>

      <SupplySimulator options={simulatorOptions} />

      <section className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-600 shadow-sm">
        <p>
          Need another owner view? Open <code className="rounded bg-slate-100 px-1 py-0.5">/u/&lt;owner_id&gt;</code>. Example: {" "}
          <Link href="/u/demo" className="font-semibold text-[var(--pivo-blue)] underline-offset-2 hover:underline">
            /u/demo
          </Link>
        </p>
      </section>
    </main>
  );
}
