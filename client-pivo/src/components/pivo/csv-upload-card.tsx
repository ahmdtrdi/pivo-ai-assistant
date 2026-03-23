"use client";

import { useMemo, useState } from "react";

type CsvSummary = {
  fileName: string;
  rowCount: number;
  columns: string[];
};

type CsvUploadCardProps = {
  title?: string;
  description?: string;
  className?: string;
  inputId?: string;
};

function parseCsvSummary(text: string, fileName: string): CsvSummary {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const header = lines[0] ?? "";
  const columns = header
    .split(",")
    .map((cell) => cell.trim())
    .filter((cell) => cell.length > 0);

  return {
    fileName,
    rowCount: Math.max(0, lines.length - 1),
    columns,
  };
}

export function CsvUploadCard({
  title = "Upload Monthly CSV",
  description = "Upload your latest POS CSV export to quickly validate file structure before processing.",
  className = "",
  inputId = "csv-upload",
}: CsvUploadCardProps) {
  const [summary, setSummary] = useState<CsvSummary | null>(null);
  const [error, setError] = useState<string>("");

  const columnPreview = useMemo(() => {
    if (!summary) {
      return [];
    }

    return summary.columns.slice(0, 8);
  }, [summary]);

  return (
    <section className={`rounded-2xl border border-slate-200 bg-white p-4 shadow-sm ${className}`}>
      <h2 className="text-base font-semibold text-slate-900">{title}</h2>
      <p className="mt-1 text-sm text-slate-600">{description}</p>

      <div className="mt-4 rounded-xl border border-dashed border-[var(--pivo-blue)]/40 bg-[var(--pivo-primary)] p-4">
        <label className="block text-sm font-medium text-slate-700" htmlFor={inputId}>
          Select CSV file
        </label>
        <input
          id={inputId}
          type="file"
          accept=".csv,text/csv"
          className="mt-2 block w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm"
          onChange={async (event) => {
            const file = event.target.files?.[0];
            setError("");
            setSummary(null);

            if (!file) {
              return;
            }

            try {
              const text = await file.text();
              const parsed = parseCsvSummary(text, file.name);

              if (parsed.columns.length === 0) {
                setError("The file looks empty or has no header row.");
                return;
              }

              setSummary(parsed);
            } catch {
              setError("Failed to read this file. Please try another CSV export.");
            }
          }}
        />
      </div>

      {error ? (
        <p className="mt-3 rounded-lg bg-[var(--pivo-coral)]/15 px-3 py-2 text-sm text-[var(--pivo-coral-ink)]">{error}</p>
      ) : null}

      {summary ? (
        <div className="mt-4 space-y-2 rounded-xl bg-[var(--pivo-primary)] p-3 text-sm text-slate-700">
          <p>
            <strong>File:</strong> {summary.fileName}
          </p>
          <p>
            <strong>Detected rows:</strong> {summary.rowCount}
          </p>
          <p>
            <strong>Detected columns:</strong> {summary.columns.length}
          </p>
          <div className="flex flex-wrap gap-2 pt-1">
            {columnPreview.map((column) => (
              <span key={column} className="rounded-full bg-white px-2.5 py-1 text-xs text-slate-700">
                {column}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  );
}
