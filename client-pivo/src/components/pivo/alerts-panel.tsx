import type { ProfitItem } from "@/types/pivo";

type AlertsPanelProps = {
  anomalyFlags: string[];
  marginAlerts: ProfitItem[];
  rankGapInsights: ProfitItem[];
};

function normalizeFlag(flag: string): string {
  return flag.replaceAll("_", " ");
}

export function AlertsPanel({ anomalyFlags, marginAlerts, rankGapInsights }: AlertsPanelProps) {
  const hasAnyAlert = anomalyFlags.length > 0 || marginAlerts.length > 0 || rankGapInsights.length > 0;

  if (!hasAnyAlert) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-600 shadow-sm">
        No critical anomalies detected today. Current operations look stable.
      </div>
    );
  }

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">Alerts and Insights</h2>
      <ul className="mt-3 space-y-2 text-sm text-slate-700">
        {anomalyFlags.map((flag) => (
          <li key={flag} className="rounded-lg bg-[var(--pivo-coral)]/10 px-3 py-2">
            {normalizeFlag(flag)}
          </li>
        ))}
        {marginAlerts.slice(0, 3).map((row) => (
          <li key={`margin-${row.sku}`} className="rounded-lg bg-[var(--pivo-amber)]/15 px-3 py-2">
            Margin for {row.sku_name} decreased by {Math.abs(row.margin_delta ?? 0).toFixed(1)} pp.
          </li>
        ))}
        {rankGapInsights.slice(0, 3).map((row) => (
          <li key={`rank-${row.sku}`} className="rounded-lg bg-[var(--pivo-blue)]/12 px-3 py-2">
            {row.sku_name} has a rank gap of {row.rank_gap}. Volume is high, but margin needs review.
          </li>
        ))}
      </ul>
    </section>
  );
}
