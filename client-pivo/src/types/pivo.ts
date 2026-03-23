export type ConfidenceTier = "green" | "yellow" | "red";
export type TrendDirection = "growing" | "stable" | "declining";

export type ForecastItem = {
  sku: string;
  sku_name: string;
  category?: string;
  selected_model?: "prophet" | "arima";
  tier: ConfidenceTier;
  tier_if_missing_3d?: ConfidenceTier;
  qty_mid: number;
  qty_low: number;
  qty_high: number;
  trend: TrendDirection;
  stockout_risk: boolean;
};

export type ProfitItem = {
  sku: string;
  sku_name: string;
  unit_price: number;
  unit_cost: number;
  revenue?: number;
  gross_profit: number;
  margin_pct: number;
  rolling_7d_sales?: number;
  rolling_7d_profit?: number;
  margin_delta?: number;
  profit_rank?: number;
  volume_rank?: number;
  rank_gap?: number;
  qty_prepared?: number | null;
  unsold_qty?: number | null;
  sell_through_rate?: number | null;
};

export type SkippedSku = {
  sku: string;
  sku_name: string;
  reason: string;
  tier: "red";
  gap_days?: number;
};

export type SalesHistoryPoint = {
  date: string;
  sku: string;
  sku_name: string;
  qty_sold: number;
  gross_profit: number;
};

export type PivoPayload = {
  owner_id: string;
  date: string;
  model_routing: "prophet_first_arima_fallback" | "skipped_missing_data";
  confidence_tier: ConfidenceTier;
  consecutive_missing_days?: number;
  forecasts: ForecastItem[];
  profit_analysis?: ProfitItem[];
  anomaly_flags?: string[];
  wa_message?: string | null;
  pwa_url?: string;
  skipped_skus?: SkippedSku[];
  history_30d?: SalesHistoryPoint[];
};

export type SimulatorOption = {
  sku: string;
  skuName: string;
  qtyMid: number;
  unitPrice: number;
  unitCost: number;
};
