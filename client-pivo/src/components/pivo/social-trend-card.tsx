import { formatNumber } from "@/lib/format";

type Platform = "tiktok" | "instagram" | "x";

type TrendRow = {
  platform: Platform;
  hashtag: string;
  usesToday: number;
};

const STATIC_TRENDS: TrendRow[] = [
  { platform: "tiktok", hashtag: "#kopisusu", usesToday: 18500 },
  { platform: "instagram", hashtag: "#eskopisusu", usesToday: 14200 },
  { platform: "x", hashtag: "#jajananviral", usesToday: 12450 },
  { platform: "tiktok", hashtag: "#ramadanmenu", usesToday: 11800 },
  { platform: "instagram", hashtag: "#kulinerkekinian", usesToday: 9700 },
  { platform: "x", hashtag: "#promoumkm", usesToday: 8600 },
];

function getSortedRows(): TrendRow[] {
  return [...STATIC_TRENDS].sort((a, b) => b.usesToday - a.usesToday);
}

function platformName(platform: Platform): string {
  if (platform === "tiktok") {
    return "TikTok";
  }

  if (platform === "instagram") {
    return "Instagram";
  }

  return "X";
}

function platformPillClass(platform: Platform): string {
  if (platform === "tiktok") {
    return "bg-slate-900 text-white";
  }

  if (platform === "instagram") {
    return "bg-[var(--pivo-coral)] text-white";
  }

  return "bg-slate-700 text-white";
}

function PlatformIcon({ platform }: { platform: Platform }) {
  if (platform === "instagram") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <rect x="4" y="4" width="16" height="16" rx="4" fill="none" stroke="currentColor" strokeWidth="2" />
        <circle cx="12" cy="12" r="3.2" fill="none" stroke="currentColor" strokeWidth="2" />
        <circle cx="17.5" cy="6.5" r="1.2" fill="currentColor" />
      </svg>
    );
  }

  if (platform === "x") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
        <path d="M5 4h4.2l3.4 4.7L16.4 4H19l-5.2 6.2L20 20h-4.2l-3.9-5.4L7.5 20H5l5.6-6.7L5 4z" fill="currentColor" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4 w-4">
      <path d="M14.1 4.3v7.4a3.4 3.4 0 10 2.2 3.2V8.4c1 .7 2.2 1.1 3.5 1.2V7.3c-1.3-.2-2.5-.9-3.3-1.9-.4-.4-.7-.8-.9-1.1h-1.5z" fill="currentColor" />
    </svg>
  );
}

export function SocialTrendCard() {
  const rows = getSortedRows();

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">F&B Trend Radar</h2>
      <p className="mt-1 text-sm text-slate-600">
        Static trend sample for today. Later we can replace this with live scraping and social API data.
      </p>

      <ul className="mt-3 max-h-[336px] space-y-2 overflow-y-auto pr-1">
        {rows.map((row, index) => (
          <li key={`${row.platform}-${row.hashtag}`} className="rounded-xl bg-[var(--pivo-primary)] px-3 py-2">
            <div className="flex items-start justify-between gap-2">
              <div>
                <p className="text-sm font-medium text-slate-900">
                  {index + 1}. {row.hashtag}
                </p>
                <p className="mt-1 text-xs text-slate-600">{formatNumber(row.usesToday)} uses today</p>
              </div>

              <span className={`inline-flex items-center gap-1 rounded-full px-2 py-1 text-[11px] font-semibold ${platformPillClass(row.platform)}`}>
                <PlatformIcon platform={row.platform} />
                {platformName(row.platform)}
              </span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
