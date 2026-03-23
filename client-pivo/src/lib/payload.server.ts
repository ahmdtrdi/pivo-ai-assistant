import "server-only";

import { DEMO_PAYLOAD } from "@/lib/demo-payload";
import type { PivoPayload } from "@/types/pivo";

type SupabaseRow = {
  owner_id: string;
  date: string;
  payload: unknown;
};

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function toPivoPayload(value: unknown): PivoPayload | null {
  if (!isObject(value)) {
    return null;
  }

  const ownerId = value.owner_id;
  const date = value.date;
  const tier = value.confidence_tier;
  const routing = value.model_routing;
  const forecasts = value.forecasts;

  if (
    typeof ownerId !== "string" ||
    typeof date !== "string" ||
    (tier !== "green" && tier !== "yellow" && tier !== "red") ||
    (routing !== "prophet_first_arima_fallback" && routing !== "skipped_missing_data") ||
    !Array.isArray(forecasts)
  ) {
    return null;
  }

  return value as PivoPayload;
}

function parseRowPayload(row: SupabaseRow): PivoPayload | null {
  const rawPayload = row.payload;

  if (typeof rawPayload === "string") {
    try {
      return toPivoPayload(JSON.parse(rawPayload));
    } catch {
      return null;
    }
  }

  return toPivoPayload(rawPayload);
}

async function fetchLatestFromSupabase(ownerId: string): Promise<PivoPayload | null> {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const anonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

  if (!url || !anonKey) {
    return null;
  }

  const query = new URLSearchParams({
    select: "owner_id,date,payload",
    owner_id: `eq.${ownerId}`,
    order: "date.desc",
    limit: "1",
  });

  const endpoint = `${url.replace(/\/$/, "")}/rest/v1/daily_payloads?${query.toString()}`;

  const response = await fetch(endpoint, {
    method: "GET",
    cache: "no-store",
    headers: {
      apikey: anonKey,
      Authorization: `Bearer ${anonKey}`,
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    return null;
  }

  const rows = (await response.json()) as SupabaseRow[];
  if (!Array.isArray(rows) || rows.length === 0) {
    return null;
  }

  return parseRowPayload(rows[0]);
}

export async function getOwnerPayload(ownerId: string): Promise<PivoPayload> {
  const latest = await fetchLatestFromSupabase(ownerId);

  if (latest) {
    return latest;
  }

  if (ownerId === DEMO_PAYLOAD.owner_id) {
    return DEMO_PAYLOAD;
  }

  return {
    ...DEMO_PAYLOAD,
    owner_id: ownerId,
    pwa_url: `pivo.app/u/${ownerId}`,
  };
}
