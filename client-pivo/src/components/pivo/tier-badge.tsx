import type { ConfidenceTier } from "@/types/pivo";

type TierBadgeProps = {
  tier: ConfidenceTier;
};

const TIER_COPY: Record<ConfidenceTier, { label: string; note: string; className: string }> = {
  green: {
    label: "Green Confidence",
    note: "Data stabil. Rekomendasi siap dipakai untuk keputusan harian.",
    className: "border-emerald-300 bg-emerald-100 text-emerald-800",
  },
  yellow: {
    label: "Yellow Confidence",
    note: "Data masih berkembang. Pakai rekomendasi sebagai estimasi awal.",
    className: "border-[var(--pivo-amber)]/40 bg-[var(--pivo-amber)]/15 text-amber-900",
  },
  red: {
    label: "Red Confidence",
    note: "Data belum cukup. Fokus catat penjualan agar prediksi kembali aktif.",
    className: "border-[var(--pivo-coral)]/45 bg-[var(--pivo-coral)]/15 text-[var(--pivo-coral-ink)]",
  },
};

export function TierBadge({ tier }: TierBadgeProps) {
  const tierCopy = TIER_COPY[tier];

  return (
    <div className={`rounded-xl border px-4 py-3 ${tierCopy.className}`}>
      <p className="text-sm font-semibold tracking-wide">{tierCopy.label}</p>
      <p className="mt-1 text-sm leading-relaxed">{tierCopy.note}</p>
    </div>
  );
}
