"use client";

import { useState } from "react";

import { CsvUploadCard } from "@/components/pivo/csv-upload-card";

type SettingsFormProps = {
  ownerId: string;
};

type DataSource = "google_sheet" | "pos_connector" | "monthly_csv";

type FormState = {
  businessName: string;
  whatsappNumber: string;
  sheetUrl: string;
  sheetTabName: string;
  posProvider: string;
  posBaseUrl: string;
  posToken: string;
  posOutletId: string;
  dataSource: DataSource;
};

function initialState(ownerId: string): FormState {
  return {
    businessName: ownerId
      .replace(/[-_]/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase()),
    whatsappNumber: "",
    sheetUrl: "",
    sheetTabName: "",
    posProvider: "",
    posBaseUrl: "",
    posToken: "",
    posOutletId: "",
    dataSource: "google_sheet",
  };
}

function panelClass(active: boolean): string {
  return active
    ? "rounded-xl border border-[var(--pivo-blue)]/35 bg-[var(--pivo-blue)]/10 p-4"
    : "rounded-xl border border-slate-200 bg-slate-50 p-4";
}

export function SettingsForm({ ownerId }: SettingsFormProps) {
  const [form, setForm] = useState<FormState>(() => initialState(ownerId));
  const [savedAt, setSavedAt] = useState<string>("");

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <h2 className="text-base font-semibold text-slate-900">Business Settings</h2>
      <p className="mt-1 text-sm text-slate-600">
        Configure your business profile and preferred data source.
      </p>

      <form
        className="mt-4 grid gap-4"
        onSubmit={(event) => {
          event.preventDefault();
          setSavedAt(new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" }));
        }}
      >
        <label className="grid gap-1 text-sm text-slate-700">
          <span className="font-medium">Business name</span>
          <input
            value={form.businessName}
            onChange={(event) => {
              setForm((prev) => ({ ...prev, businessName: event.target.value }));
            }}
            className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
            placeholder="My Coffee Shop"
          />
        </label>

        <label className="grid gap-1 text-sm text-slate-700">
          <span className="font-medium">WhatsApp Business number</span>
          <input
            value={form.whatsappNumber}
            onChange={(event) => {
              setForm((prev) => ({ ...prev, whatsappNumber: event.target.value }));
            }}
            className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
            placeholder="62812xxxxxxxx"
          />
        </label>

        <label className="grid gap-1 text-sm text-slate-700">
          <span className="font-medium">Primary data source</span>
          <select
            value={form.dataSource}
            onChange={(event) => {
              setForm((prev) => ({ ...prev, dataSource: event.target.value as DataSource }));
            }}
            className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
          >
            <option value="google_sheet">Google Sheet</option>
            <option value="pos_connector">POS connector</option>
            <option value="monthly_csv">Monthly CSV upload</option>
          </select>
        </label>

        <section className={panelClass(form.dataSource === "google_sheet")}>
          <h3 className="text-sm font-semibold text-slate-900">Google Sheet connection</h3>
          <p className="mt-1 text-sm text-slate-600">Use this when your cashier exports sales into a shared spreadsheet.</p>

          {form.dataSource === "google_sheet" ? (
            <div className="mt-3 grid gap-3">
              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">Google Sheet URL</span>
                <input
                  value={form.sheetUrl}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, sheetUrl: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                  placeholder="https://docs.google.com/spreadsheets/..."
                />
              </label>

              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">Worksheet/tab name (optional)</span>
                <input
                  value={form.sheetTabName}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, sheetTabName: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                  placeholder="March 2026 Sales"
                />
              </label>
            </div>
          ) : null}
        </section>

        <section className={panelClass(form.dataSource === "pos_connector")}>
          <h3 className="text-sm font-semibold text-slate-900">POS API connection</h3>
          <p className="mt-1 text-sm text-slate-600">
            Most POS providers require credentials, not only a link. Standard inputs are API token plus outlet/store ID.
          </p>

          {form.dataSource === "pos_connector" ? (
            <div className="mt-3 grid gap-3">
              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">POS provider</span>
                <select
                  value={form.posProvider}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, posProvider: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                >
                  <option value="">Select provider</option>
                  <option value="moka">Moka</option>
                  <option value="majoo">Majoo</option>
                  <option value="pawoon">Pawoon</option>
                  <option value="olsera">Olsera</option>
                  <option value="other">Other</option>
                </select>
              </label>

              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">API base URL (optional)</span>
                <input
                  value={form.posBaseUrl}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, posBaseUrl: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                  placeholder="https://api.provider.com/v1"
                />
              </label>

              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">API token / secret key</span>
                <input
                  type="password"
                  value={form.posToken}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, posToken: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                  placeholder="Enter credential from POS admin"
                />
              </label>

              <label className="grid gap-1 text-sm text-slate-700">
                <span className="font-medium">Outlet/store ID</span>
                <input
                  value={form.posOutletId}
                  onChange={(event) => {
                    setForm((prev) => ({ ...prev, posOutletId: event.target.value }));
                  }}
                  className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
                  placeholder="outlet-001"
                />
              </label>

              <p className="rounded-lg bg-white px-3 py-2 text-sm text-slate-600">
                Integration note: If a provider supports OAuth, this section will later be replaced by a &quot;Connect POS&quot;
                button. For now we keep a universal token + outlet format.
              </p>
            </div>
          ) : null}
        </section>

        <section className={panelClass(form.dataSource === "monthly_csv")}>
          <h3 className="text-sm font-semibold text-slate-900">Monthly CSV upload</h3>
          <p className="mt-1 text-sm text-slate-600">
            Use this when sales data is exported manually from cashier/POS every month.
          </p>

          {form.dataSource === "monthly_csv" ? (
            <CsvUploadCard
              className="mt-3 border-slate-300"
              title="Upload monthly CSV file"
              description="Upload one monthly sales CSV and we will validate structure before backend processing."
              inputId="monthly-csv-upload"
            />
          ) : null}
        </section>

        <div className="flex flex-wrap items-center gap-3 pt-1">
          <button
            type="submit"
            className="rounded-xl bg-[var(--pivo-navy)] px-4 py-2 text-sm font-semibold text-white transition hover:bg-[var(--pivo-blue)]"
          >
            Save settings
          </button>
          {savedAt ? <p className="text-sm text-slate-600">Saved at {savedAt}</p> : null}
        </div>
      </form>
    </section>
  );
}
