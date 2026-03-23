"use client";

import { useState } from "react";

type SettingsFormProps = {
  ownerId: string;
};

type DataSource = "google_sheet" | "pos_connector" | "monthly_csv";

type FormState = {
  businessName: string;
  whatsappNumber: string;
  sheetUrl: string;
  posName: string;
  dataSource: DataSource;
};

function initialState(ownerId: string): FormState {
  return {
    businessName: ownerId
      .replace(/[-_]/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase()),
    whatsappNumber: "",
    sheetUrl: "",
    posName: "",
    dataSource: "google_sheet",
  };
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

        <label className="grid gap-1 text-sm text-slate-700">
          <span className="font-medium">Google Sheet URL (optional)</span>
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
          <span className="font-medium">POS system name (optional)</span>
          <input
            value={form.posName}
            onChange={(event) => {
              setForm((prev) => ({ ...prev, posName: event.target.value }));
            }}
            className="rounded-xl border border-slate-300 px-3 py-2 outline-none focus:border-[var(--pivo-blue)]"
            placeholder="Moka, Majoo, Pawoon, etc."
          />
        </label>

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
