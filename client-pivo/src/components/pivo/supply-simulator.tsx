"use client";

import { useMemo, useState } from "react";

import { formatIdr, formatNumber } from "@/lib/format";
import type { SimulatorOption } from "@/types/pivo";

type SupplySimulatorProps = {
  options: SimulatorOption[];
};

type SimulatorResult = {
  expectedProfit: number;
  opportunityLoss: number;
  wasteCost: number;
  condition: "exact" | "under" | "over";
};

function computeResult(option: SimulatorOption, unitsPrepared: number): SimulatorResult {
  const demand = option.qtyMid;
  const marginPerUnit = option.unitPrice - option.unitCost;

  if (unitsPrepared === demand) {
    return {
      expectedProfit: marginPerUnit * unitsPrepared,
      opportunityLoss: 0,
      wasteCost: 0,
      condition: "exact",
    };
  }

  if (unitsPrepared < demand) {
    return {
      expectedProfit: marginPerUnit * unitsPrepared,
      opportunityLoss: (demand - unitsPrepared) * marginPerUnit,
      wasteCost: 0,
      condition: "under",
    };
  }

  return {
    expectedProfit: marginPerUnit * demand,
    opportunityLoss: 0,
    wasteCost: (unitsPrepared - demand) * option.unitCost,
    condition: "over",
  };
}

const CONDITION_COPY: Record<SimulatorResult["condition"], string> = {
  exact: "Preparation matches expected demand.",
  under: "You are preparing too little and may miss potential sales.",
  over: "You are preparing too much and waste cost is increasing.",
};

export function SupplySimulator({ options }: SupplySimulatorProps) {
  const [selectedSku, setSelectedSku] = useState(options[0]?.sku ?? "");
  const [unitsPrepared, setUnitsPrepared] = useState(
    Math.max(0, Math.round(options[0]?.qtyMid ?? 0)),
  );

  const selectedOption = useMemo(() => {
    return options.find((option) => option.sku === selectedSku) ?? options[0];
  }, [options, selectedSku]);

  if (options.length === 0 || !selectedOption) {
    return (
      <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <h2 className="text-base font-semibold text-slate-900">Supply Simulator</h2>
        <p className="mt-2 text-sm text-slate-600">
          This section becomes available when your payload includes forecast data and product cost data.
        </p>
      </section>
    );
  }

  const maxUnits = Math.max(10, Math.ceil(selectedOption.qtyMid * 2));
  const result = computeResult(selectedOption, unitsPrepared);

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">Supply Simulator</h2>
      <p className="mt-1 text-sm text-slate-600">
        Plan your production before execution and compare risk in real time.
      </p>

      <div className="mt-4 grid gap-4 md:grid-cols-2">
        <label className="space-y-1.5 text-sm text-slate-700">
          <span className="font-medium">Select product</span>
          <select
            value={selectedOption.sku}
            onChange={(event) => {
              const nextSku = event.target.value;
              const nextOption = options.find((option) => option.sku === nextSku);

              setSelectedSku(nextSku);
              setUnitsPrepared(Math.max(0, Math.round(nextOption?.qtyMid ?? 0)));
            }}
            className="w-full rounded-xl border border-slate-300 bg-white px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
          >
            {options.map((option) => (
              <option key={option.sku} value={option.sku}>
                {option.skuName}
              </option>
            ))}
          </select>
        </label>

        <div className="rounded-xl bg-[var(--pivo-primary)] px-3 py-2 text-sm text-slate-700">
          <p>
            Expected demand: <strong>{formatNumber(selectedOption.qtyMid)}</strong> units
          </p>
          <p>
            Margin per unit: <strong>{formatIdr(selectedOption.unitPrice - selectedOption.unitCost)}</strong>
          </p>
        </div>
      </div>

      <div className="mt-4">
        <div className="flex items-center justify-between text-sm text-slate-700">
          <span className="font-medium">Units to prepare (Y)</span>
          <strong>{formatNumber(unitsPrepared)}</strong>
        </div>
        <input
          className="mt-2 h-2 w-full cursor-pointer appearance-none rounded-full bg-[var(--pivo-navy)]/20"
          type="range"
          min={0}
          max={maxUnits}
          step={1}
          value={unitsPrepared}
          onChange={(event) => {
            setUnitsPrepared(Number(event.target.value));
          }}
        />
        <p className="mt-1 text-xs text-slate-500">Simulation range: 0 to {formatNumber(maxUnits)} units.</p>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-3">
        <article className="rounded-xl bg-[var(--pivo-navy)] p-3 text-white">
          <p className="text-xs uppercase tracking-wide text-white/80">Estimated Profit</p>
          <p className="mt-1 text-sm font-semibold">{formatIdr(result.expectedProfit)}</p>
        </article>
        <article className="rounded-xl bg-[var(--pivo-amber)]/20 p-3">
          <p className="text-xs uppercase tracking-wide text-amber-900">Opportunity Loss</p>
          <p className="mt-1 text-sm font-semibold text-amber-900">{formatIdr(result.opportunityLoss)}</p>
        </article>
        <article className="rounded-xl bg-[var(--pivo-coral)]/20 p-3">
          <p className="text-xs uppercase tracking-wide text-[var(--pivo-coral-ink)]">Waste Cost</p>
          <p className="mt-1 text-sm font-semibold text-[var(--pivo-coral-ink)]">{formatIdr(result.wasteCost)}</p>
        </article>
      </div>

      <p className="mt-3 rounded-xl bg-[var(--pivo-blue)]/12 px-3 py-2 text-sm text-slate-700">
        {CONDITION_COPY[result.condition]}
      </p>
    </section>
  );
}
