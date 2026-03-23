import { SupplySimulator } from "@/components/pivo/supply-simulator";
import { getSimulatorOptions } from "@/lib/dashboard";
import { getOwnerPayload } from "@/lib/payload.server";

export const revalidate = 0;

type OwnerSimulatorPageProps = {
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerSimulatorPage({ params }: OwnerSimulatorPageProps) {
  const { ownerId } = await params;
  const payload = await getOwnerPayload(ownerId);
  const simulatorOptions = getSimulatorOptions(payload);

  return (
    <section className="space-y-6">
      <section className="rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">Simulator</p>
        <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">Production Planning Simulator</h1>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-700 sm:text-base">
          Test different preparation quantities before production starts. Compare expected profit, missed opportunity, and waste cost.
        </p>
      </section>

      <SupplySimulator options={simulatorOptions} />
    </section>
  );
}
