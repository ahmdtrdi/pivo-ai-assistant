import { formatIdr, formatNumber } from "@/lib/format";
import type { ProfitItem } from "@/types/pivo";

type TopProfitListProps = {
  rows: ProfitItem[];
};

export function TopProfitList({ rows }: TopProfitListProps) {
  if (rows.length === 0) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white p-4 text-sm text-slate-600 shadow-sm">
        Profit data is not available yet for today.
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">Top Profit Products</h2>
      <ul className="mt-3 space-y-3">
        {rows.map((row, index) => (
          <li key={row.sku} className="rounded-xl bg-[var(--pivo-primary)] p-3">
            <div className="flex items-center justify-between gap-3">
              <p className="font-medium text-slate-800">
                {index + 1}. {row.sku_name}
              </p>
              <span className="text-sm font-semibold text-[var(--pivo-navy)]">{formatNumber(row.margin_pct)}%</span>
            </div>
            <p className="mt-1 text-sm text-slate-600">Gross profit {formatIdr(row.gross_profit)}</p>
          </li>
        ))}
      </ul>
    </div>
  );
}
