import { formatIdr, formatNumber } from "@/lib/format";
import type { PivoPayload, ProfitItem } from "@/types/pivo";

import { TierBadge } from "@/components/pivo/tier-badge";

type DailyDecisionBriefProps = {
  ownerName: string;
  payload: PivoPayload;
  topProfit?: ProfitItem;
  marginAlerts: ProfitItem[];
};

const ROUTING_LABEL: Record<PivoPayload["model_routing"], string> = {
  prophet_first_arima_fallback: "Prophet first, ARIMA fallback",
  skipped_missing_data: "Prediction skipped due to missing data",
};

function toFlagLabel(value: string): string {
  return value.replaceAll("_", " ");
}

function getConfidenceReason(payload: PivoPayload): string {
  const missingDays = payload.consecutive_missing_days ?? 0;

  if (payload.confidence_tier === "green") {
    return "Recent records are complete and stable. You can follow this plan with stronger confidence.";
  }

  if (payload.confidence_tier === "yellow") {
    if (missingDays > 0) {
      return `Early estimate mode is active because ${missingDays} recent day${missingDays > 1 ? "s are" : " is"} missing.`;
    }

    return "Early estimate mode is active because recent data is still limited.";
  }

  return "Prediction quality is currently low due to missing recent records. Keep logging sales to reactivate stronger guidance.";
}

function getPriorityForecast(payload: PivoPayload) {
  const rows = payload.forecasts ?? [];

  if (rows.length === 0) {
    return null;
  }

  return [...rows].sort((a, b) => {
    if (a.stockout_risk !== b.stockout_risk) {
      return Number(b.stockout_risk) - Number(a.stockout_risk);
    }

    return b.qty_mid - a.qty_mid;
  })[0];
}

export function DailyDecisionBrief({ ownerName, payload, topProfit, marginAlerts }: DailyDecisionBriefProps) {
  const priorityForecast = getPriorityForecast(payload);
  const topAnomaly = payload.anomaly_flags?.[0];
  const strongestMarginAlert = marginAlerts[0];
  const confidenceReason = getConfidenceReason(payload);
  const insightRows = [
    priorityForecast
      ? `Prepare ${formatNumber(priorityForecast.qty_mid)} units of ${priorityForecast.sku_name} (range ${formatNumber(
          priorityForecast.qty_low,
        )}-${formatNumber(priorityForecast.qty_high)}).`
      : "No forecast is available yet. Keep recording daily sales to activate recommendations.",
    topProfit
      ? `Most profitable product today: ${topProfit.sku_name} with ${formatIdr(topProfit.gross_profit)} gross profit and ${formatNumber(topProfit.margin_pct)}% margin.`
      : "Profit ranking will appear after enough sales records are available.",
    strongestMarginAlert
      ? `Margin warning: ${strongestMarginAlert.sku_name} is down ${formatNumber(Math.abs(strongestMarginAlert.margin_delta ?? 0))} percentage points.`
      : "No major margin decline detected from current records.",
    topAnomaly ? `Priority anomaly: ${toFlagLabel(topAnomaly)}.` : "No critical anomaly is currently flagged.",
  ];

  return (
    <section className="overflow-hidden rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">AI Decision Brief</p>
          <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">
            Hi {ownerName}, here is today&apos;s decision summary.
          </h1>
          <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-700 sm:text-base">
            Built from your latest sales patterns, margin behavior, and anomaly checks.
          </p>
          <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
            <span className="rounded-full bg-white px-3 py-1">Date: {payload.date}</span>
            <span className="rounded-full bg-white px-3 py-1">Routing: {ROUTING_LABEL[payload.model_routing]}</span>
          </div>
        </div>

        <div className="w-full max-w-sm lg:shrink-0">
          <TierBadge tier={payload.confidence_tier} />
        </div>
      </div>

      <div className="mt-4 grid gap-3 xl:grid-cols-[1.25fr_0.75fr]">
        <ul className="grid gap-3 text-sm text-slate-700 sm:grid-cols-2">
          {insightRows.map((insight) => (
            <li key={insight} className="min-h-[84px] rounded-xl border border-white/60 bg-white/85 px-3 py-3 leading-relaxed">
              {insight}
            </li>
          ))}
        </ul>

        <div className="rounded-2xl border border-slate-200 bg-white/90 p-4 text-sm text-slate-700">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-800">
            Why Confidence Is {payload.confidence_tier}
          </h2>
          <p className="mt-2 leading-relaxed">{confidenceReason}</p>

          {payload.wa_message ? (
            <>
              <p className="mt-4 text-sm font-semibold text-slate-800">Suggested owner message</p>
              <p className="mt-1 rounded-lg bg-[var(--pivo-primary)] px-3 py-2 italic text-slate-700">{payload.wa_message}</p>
            </>
          ) : null}
        </div>
      </div>
    </section>
  );
}
