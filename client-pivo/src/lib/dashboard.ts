import type { PivoPayload, ProfitItem, SimulatorOption } from "@/types/pivo";

export function getTopProfits(rows: ProfitItem[], limit = 5): ProfitItem[] {
  return [...rows].sort((a, b) => b.gross_profit - a.gross_profit).slice(0, limit);
}

export function getMarginAlerts(rows: ProfitItem[]): ProfitItem[] {
  return rows
    .filter((row) => typeof row.margin_delta === "number" && row.margin_delta < 0)
    .sort((a, b) => (a.margin_delta ?? 0) - (b.margin_delta ?? 0));
}

export function getRankGapInsights(rows: ProfitItem[]): ProfitItem[] {
  return rows
    .filter((row) => typeof row.rank_gap === "number" && row.rank_gap >= 2)
    .sort((a, b) => (b.rank_gap ?? 0) - (a.rank_gap ?? 0));
}

export function getSimulatorOptions(payload: PivoPayload): SimulatorOption[] {
  const forecastRows = payload.forecasts ?? [];
  const profitRows = payload.profit_analysis ?? [];

  const profitMap = new Map<string, ProfitItem>();
  for (const row of profitRows) {
    profitMap.set(row.sku, row);
  }

  const options: SimulatorOption[] = [];
  for (const forecast of forecastRows) {
    const profit = profitMap.get(forecast.sku);

    if (!profit || typeof profit.unit_price !== "number" || typeof profit.unit_cost !== "number") {
      continue;
    }

    options.push({
      sku: forecast.sku,
      skuName: forecast.sku_name,
      qtyMid: forecast.qty_mid,
      unitPrice: profit.unit_price,
      unitCost: profit.unit_cost,
    });
  }

  return options.sort((a, b) => b.qtyMid - a.qtyMid);
}
