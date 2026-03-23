import { formatNumber } from "@/lib/format";
import type { ForecastItem } from "@/types/pivo";

type ForecastCardProps = {
  forecast: ForecastItem;
};

const TREND_LABEL: Record<ForecastItem["trend"], string> = {
  growing: "Demand is rising",
  stable: "Demand is stable",
  declining: "Demand is declining",
};

export function ForecastCard({ forecast }: ForecastCardProps) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm transition hover:shadow-md">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-slate-900">{forecast.sku_name}</h3>
          <p className="mt-1 text-sm text-slate-600">{TREND_LABEL[forecast.trend]}</p>
        </div>
        {forecast.stockout_risk ? (
          <span className="rounded-full border border-[var(--pivo-coral)]/50 bg-[var(--pivo-coral)]/10 px-2.5 py-1 text-xs font-semibold text-[var(--pivo-coral-ink)]">
            Stockout risk
          </span>
        ) : null}
      </div>

      <div className="mt-4 grid grid-cols-3 gap-2 text-center text-sm">
        <div className="rounded-lg bg-[var(--pivo-primary)] px-2 py-2">
          <p className="text-xs text-slate-500">Low</p>
          <p className="font-semibold text-slate-800">{formatNumber(forecast.qty_low)}</p>
        </div>
        <div className="rounded-lg bg-[var(--pivo-navy)]/10 px-2 py-2">
          <p className="text-xs text-slate-600">Recommended</p>
          <p className="font-semibold text-[var(--pivo-navy)]">{formatNumber(forecast.qty_mid)}</p>
        </div>
        <div className="rounded-lg bg-[var(--pivo-primary)] px-2 py-2">
          <p className="text-xs text-slate-500">High</p>
          <p className="font-semibold text-slate-800">{formatNumber(forecast.qty_high)}</p>
        </div>
      </div>
    </article>
  );
}
