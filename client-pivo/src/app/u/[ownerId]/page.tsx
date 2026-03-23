import { AlertsPanel } from "@/components/pivo/alerts-panel";
import { DailyDecisionBrief } from "@/components/pivo/daily-decision-brief";
import { ForecastCard } from "@/components/pivo/forecast-card";
import { SocialTrendCard } from "@/components/pivo/social-trend-card";
import { TopProfitList } from "@/components/pivo/top-profit-list";
import { getMarginAlerts, getRankGapInsights, getTopProfits } from "@/lib/dashboard";
import { titleCaseFromId } from "@/lib/format";
import { getOwnerPayload } from "@/lib/payload.server";

export const revalidate = 0;

type OwnerDashboardPageProps = {
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerDashboardPage({ params }: OwnerDashboardPageProps) {
  const { ownerId } = await params;
  const payload = await getOwnerPayload(ownerId);

  const profitRows = payload.profit_analysis ?? [];
  const topProfits = getTopProfits(profitRows, 5);
  const marginAlerts = getMarginAlerts(profitRows);
  const rankGapInsights = getRankGapInsights(profitRows);
  const displayOwnerName = titleCaseFromId(ownerId);
  const greetingName = ownerId === "demo" ? "Rina" : displayOwnerName;

  return (
    <section className="space-y-6">
      <DailyDecisionBrief ownerName={greetingName} payload={payload} topProfit={topProfits[0]} marginAlerts={marginAlerts} />

      <section className="grid gap-4 xl:grid-cols-12">
        <div className="xl:col-span-7">
          <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">Daily Production Recommendations</h2>

            {payload.forecasts.length === 0 ? (
              <div className="mt-3 rounded-2xl border border-[var(--pivo-coral)]/35 bg-[var(--pivo-coral)]/10 p-4 text-sm text-[var(--pivo-coral-ink)]">
                No SKU prediction is available today yet. Continue recording daily sales to reactivate forecasting.
              </div>
            ) : (
              <div className="mt-3 grid gap-3 md:grid-cols-2">
                {payload.forecasts.map((forecast) => (
                  <ForecastCard key={forecast.sku} forecast={forecast} />
                ))}
              </div>
            )}
          </section>
        </div>

        <div className="xl:col-span-5">
          <SocialTrendCard />
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <TopProfitList rows={topProfits} />
        <AlertsPanel
          anomalyFlags={payload.anomaly_flags ?? []}
          marginAlerts={marginAlerts}
          rankGapInsights={rankGapInsights}
        />
      </section>
    </section>
  );
}
