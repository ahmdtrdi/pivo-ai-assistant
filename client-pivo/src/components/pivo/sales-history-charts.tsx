"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatIdr, formatNumber } from "@/lib/format";
import {
  getHistorySkuOptions,
  getQtyLineSeries,
  getWeeklyProfitBars,
  normalizeHistoryRows,
} from "@/lib/history";
import type { SalesHistoryPoint } from "@/types/pivo";

type SalesHistoryChartsProps = {
  rows: SalesHistoryPoint[];
};

function formatDateTick(isoDate: string): string {
  const date = new Date(`${isoDate}T00:00:00Z`);

  return new Intl.DateTimeFormat("id-ID", {
    day: "2-digit",
    month: "short",
  }).format(date);
}

function formatTooltipNumeric(value: unknown): string {
  const asNumber = Number(value);
  return formatNumber(Number.isFinite(asNumber) ? asNumber : 0);
}

function formatTooltipCurrency(value: unknown): string {
  const asNumber = Number(value);
  return formatIdr(Number.isFinite(asNumber) ? asNumber : 0);
}

function formatTooltipLabel(label: unknown): string {
  if (typeof label !== "string") {
    return "";
  }

  return formatDateTick(label);
}

export function SalesHistoryCharts({ rows }: SalesHistoryChartsProps) {
  const normalizedRows = useMemo(() => normalizeHistoryRows(rows), [rows]);
  const skuOptions = useMemo(() => getHistorySkuOptions(normalizedRows), [normalizedRows]);
  const [selectedSku, setSelectedSku] = useState(skuOptions[0]?.sku ?? "");

  if (normalizedRows.length === 0 || skuOptions.length === 0) {
    return (
      <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Sales History</h2>
        <p className="mt-2 text-sm text-slate-600">
          Data history 30 hari belum tersedia di payload. Setelah backend mengirim `history_30d`, chart akan tampil otomatis.
        </p>
      </section>
    );
  }

  const currentSku = skuOptions.find((option) => option.sku === selectedSku) ?? skuOptions[0];
  const lineData = getQtyLineSeries(normalizedRows, currentSku.sku);
  const weeklyBarData = getWeeklyProfitBars(normalizedRows);

  return (
    <section className="space-y-5">
      <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-base font-semibold text-slate-900">Qty Sold - Last 30 Days</h2>
            <p className="text-sm text-slate-600">Per SKU, update harian untuk membaca pola permintaan.</p>
          </div>
          <label className="text-sm text-slate-700">
            <span className="mb-1 block font-medium">Pilih SKU</span>
            <select
              className="w-full min-w-44 rounded-xl border border-slate-300 bg-white px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
              value={currentSku.sku}
              onChange={(event) => {
                setSelectedSku(event.target.value);
              }}
            >
              {skuOptions.map((option) => (
                <option key={option.sku} value={option.sku}>
                  {option.skuName}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="mt-4 h-80 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={lineData} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="4 4" stroke="#cbd5e1" />
              <XAxis dataKey="date" tickFormatter={formatDateTick} tick={{ fontSize: 12 }} minTickGap={24} />
              <YAxis tick={{ fontSize: 12 }} width={38} />
              <Tooltip
                formatter={(value: unknown) => [formatTooltipNumeric(value), "Qty Sold"]}
                labelFormatter={(label: unknown) => formatTooltipLabel(label)}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="qtySold"
                name={currentSku.skuName}
                stroke="var(--pivo-blue)"
                strokeWidth={2.5}
                dot={false}
                activeDot={{ r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </article>

      <article className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Gross Profit Comparison</h2>
        <p className="text-sm text-slate-600">Perbandingan total gross profit 7 hari terakhir vs 7 hari sebelumnya.</p>

        <div className="mt-4 h-72 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={weeklyBarData} margin={{ top: 8, right: 12, left: 0, bottom: 8 }}>
              <CartesianGrid strokeDasharray="4 4" stroke="#cbd5e1" />
              <XAxis dataKey="label" tick={{ fontSize: 12 }} />
              <YAxis tickFormatter={(value: number) => formatNumber(value / 1_000)} tick={{ fontSize: 12 }} width={42} />
              <Tooltip formatter={(value: unknown) => formatTooltipCurrency(value)} />
              <Legend />
              <Bar dataKey="grossProfit" name="Gross Profit" fill="var(--pivo-navy)" radius={[6, 6, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </article>
    </section>
  );
}
