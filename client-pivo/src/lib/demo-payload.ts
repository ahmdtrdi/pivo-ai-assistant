import type { PivoPayload, SalesHistoryPoint } from "@/types/pivo";

type HistorySeed = {
  sku: string;
  skuName: string;
  unitPrice: number;
  unitCost: number;
  baseQty: number;
  wave: number;
  phase: number;
};

const HISTORY_SEEDS: HistorySeed[] = [
  {
    sku: "kopi_susu",
    skuName: "Kopi Susu",
    unitPrice: 12000,
    unitCost: 5400,
    baseQty: 44,
    wave: 6,
    phase: 0,
  },
  {
    sku: "es_teh",
    skuName: "Es Teh",
    unitPrice: 7000,
    unitCost: 2800,
    baseQty: 30,
    wave: 4,
    phase: 1,
  },
  {
    sku: "nasi_goreng",
    skuName: "Nasi Goreng",
    unitPrice: 18000,
    unitCost: 9600,
    baseQty: 24,
    wave: 5,
    phase: 2,
  },
];

function toIsoDate(baseDate: Date, index: number): string {
  const next = new Date(baseDate);
  next.setDate(baseDate.getDate() + index);
  return next.toISOString().slice(0, 10);
}

function buildHistory30d(): SalesHistoryPoint[] {
  const startDate = new Date("2026-02-22T00:00:00.000Z");
  const days = 30;

  const rows: SalesHistoryPoint[] = [];
  for (let dayIndex = 0; dayIndex < days; dayIndex += 1) {
    const date = toIsoDate(startDate, dayIndex);
    const weekendBoost = dayIndex % 7 === 5 || dayIndex % 7 === 6 ? 3 : 0;

    for (const seed of HISTORY_SEEDS) {
      const seasonal = Math.sin((dayIndex + seed.phase) / 3) * seed.wave;
      const qtySold = Math.max(8, Math.round(seed.baseQty + seasonal + weekendBoost));
      const grossProfit = qtySold * (seed.unitPrice - seed.unitCost);

      rows.push({
        date,
        sku: seed.sku,
        sku_name: seed.skuName,
        qty_sold: qtySold,
        gross_profit: grossProfit,
      });
    }
  }

  return rows;
}

const HISTORY_30D = buildHistory30d();

export const DEMO_PAYLOAD: PivoPayload = {
  owner_id: "demo",
  date: "2026-03-23",
  model_routing: "prophet_first_arima_fallback",
  confidence_tier: "yellow",
  consecutive_missing_days: 1,
  forecasts: [
    {
      sku: "kopi_susu",
      sku_name: "Kopi Susu",
      tier: "yellow",
      qty_mid: 45,
      qty_low: 38,
      qty_high: 54,
      trend: "growing",
      stockout_risk: true,
    },
    {
      sku: "es_teh",
      sku_name: "Es Teh",
      tier: "yellow",
      qty_mid: 30,
      qty_low: 24,
      qty_high: 35,
      trend: "stable",
      stockout_risk: false,
    },
    {
      sku: "nasi_goreng",
      sku_name: "Nasi Goreng",
      tier: "yellow",
      qty_mid: 22,
      qty_low: 16,
      qty_high: 28,
      trend: "declining",
      stockout_risk: false,
    },
  ],
  profit_analysis: [
    {
      sku: "kopi_susu",
      sku_name: "Kopi Susu",
      unit_price: 12000,
      unit_cost: 5400,
      revenue: 540000,
      gross_profit: 297000,
      margin_pct: 55,
      rolling_7d_sales: 42,
      rolling_7d_profit: 260000,
      margin_delta: 1.8,
      profit_rank: 1,
      volume_rank: 1,
      rank_gap: 0,
      qty_prepared: 50,
      unsold_qty: 5,
      sell_through_rate: 90,
    },
    {
      sku: "es_teh",
      sku_name: "Es Teh",
      unit_price: 7000,
      unit_cost: 2800,
      revenue: 210000,
      gross_profit: 126000,
      margin_pct: 60,
      rolling_7d_sales: 28,
      rolling_7d_profit: 108000,
      margin_delta: -1.1,
      profit_rank: 2,
      volume_rank: 2,
      rank_gap: 0,
      qty_prepared: 34,
      unsold_qty: 4,
      sell_through_rate: 88.2,
    },
    {
      sku: "nasi_goreng",
      sku_name: "Nasi Goreng",
      unit_price: 18000,
      unit_cost: 9600,
      revenue: 396000,
      gross_profit: 184800,
      margin_pct: 46,
      rolling_7d_sales: 25,
      rolling_7d_profit: 172000,
      margin_delta: -2.4,
      profit_rank: 3,
      volume_rank: 1,
      rank_gap: -2,
      qty_prepared: null,
      unsold_qty: null,
      sell_through_rate: null,
    },
  ],
  anomaly_flags: ["margin_drop_nasi_goreng", "stockout_risk_kopi_susu"],
  wa_message:
    "Prediksi awal: Kopi Susu sekitar 45 cup besok. Terus catat supaya makin akurat.",
  pwa_url: "pivo.app/u/demo",
  skipped_skus: [],
  history_30d: HISTORY_30D,
};
