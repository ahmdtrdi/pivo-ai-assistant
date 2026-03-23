import type { SalesHistoryPoint } from "@/types/pivo";

export type HistorySkuOption = {
  sku: string;
  skuName: string;
};

export type QtyLinePoint = {
  date: string;
  qtySold: number;
};

export type WeeklyProfitPoint = {
  label: "Last Week" | "This Week";
  grossProfit: number;
};

function toEpochDay(isoDate: string): number {
  const ms = Date.parse(`${isoDate}T00:00:00Z`);
  return Number.isNaN(ms) ? Number.NaN : Math.floor(ms / 86_400_000);
}

export function normalizeHistoryRows(rows: SalesHistoryPoint[]): SalesHistoryPoint[] {
  return rows
    .filter((row) =>
      typeof row.date === "string"
      && typeof row.sku === "string"
      && typeof row.sku_name === "string"
      && Number.isFinite(row.qty_sold)
      && Number.isFinite(row.gross_profit),
    )
    .sort((a, b) => a.date.localeCompare(b.date));
}

export function getHistorySkuOptions(rows: SalesHistoryPoint[]): HistorySkuOption[] {
  const seen = new Map<string, string>();

  for (const row of rows) {
    if (!seen.has(row.sku)) {
      seen.set(row.sku, row.sku_name);
    }
  }

  return [...seen.entries()].map(([sku, skuName]) => ({ sku, skuName }));
}

export function getQtyLineSeries(rows: SalesHistoryPoint[], sku: string): QtyLinePoint[] {
  const filtered = rows.filter((row) => row.sku === sku);

  return filtered.map((row) => ({
    date: row.date,
    qtySold: row.qty_sold,
  }));
}

export function getWeeklyProfitBars(rows: SalesHistoryPoint[]): WeeklyProfitPoint[] {
  if (rows.length === 0) {
    return [
      { label: "Last Week", grossProfit: 0 },
      { label: "This Week", grossProfit: 0 },
    ];
  }

  const dateToProfit = new Map<string, number>();
  for (const row of rows) {
    dateToProfit.set(row.date, (dateToProfit.get(row.date) ?? 0) + row.gross_profit);
  }

  const allDates = [...dateToProfit.keys()].sort((a, b) => a.localeCompare(b));
  const latest = allDates[allDates.length - 1];
  const latestEpoch = toEpochDay(latest);

  if (Number.isNaN(latestEpoch)) {
    return [
      { label: "Last Week", grossProfit: 0 },
      { label: "This Week", grossProfit: 0 },
    ];
  }

  const thisWeekStart = latestEpoch - 6;
  const prevWeekStart = latestEpoch - 13;
  const prevWeekEnd = latestEpoch - 7;

  let thisWeekProfit = 0;
  let prevWeekProfit = 0;

  for (const [date, grossProfit] of dateToProfit.entries()) {
    const epoch = toEpochDay(date);
    if (Number.isNaN(epoch)) {
      continue;
    }

    if (epoch >= thisWeekStart && epoch <= latestEpoch) {
      thisWeekProfit += grossProfit;
    } else if (epoch >= prevWeekStart && epoch <= prevWeekEnd) {
      prevWeekProfit += grossProfit;
    }
  }

  return [
    { label: "Last Week", grossProfit: prevWeekProfit },
    { label: "This Week", grossProfit: thisWeekProfit },
  ];
}
