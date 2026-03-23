import { SettingsForm } from "@/components/pivo/settings-form";

export const revalidate = 0;

type OwnerSettingsPageProps = {
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerSettingsPage({ params }: OwnerSettingsPageProps) {
  const { ownerId } = await params;

  return (
    <section className="space-y-6">
      <section className="rounded-3xl border border-[var(--pivo-blue)]/25 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-5 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">Settings</p>
        <h1 className="mt-2 text-2xl font-bold text-slate-900 sm:text-3xl">Business and Data Connections</h1>
        <p className="mt-2 max-w-3xl text-sm leading-relaxed text-slate-700 sm:text-base">
          Add your business profile, WhatsApp contact, and preferred data source so backend services can process your daily insights.
        </p>
      </section>

      <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <SettingsForm ownerId={ownerId} />

        <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <h2 className="text-base font-semibold text-slate-900">Data Source Playbook</h2>
          <ul className="mt-3 space-y-2 text-sm text-slate-700">
            <li className="rounded-lg bg-[var(--pivo-primary)] px-3 py-2">
              Google Sheet mode: connect spreadsheet URL and optional tab name.
            </li>
            <li className="rounded-lg bg-[var(--pivo-primary)] px-3 py-2">
              POS API mode: connect with API token/secret + outlet/store ID, and provider base URL when available.
            </li>
            <li className="rounded-lg bg-[var(--pivo-primary)] px-3 py-2">
              Monthly CSV mode: upload one monthly file manually when automation is not ready yet.
            </li>
            <li className="rounded-lg bg-[var(--pivo-primary)] px-3 py-2">Set WhatsApp Business destination for daily decision delivery.</li>
          </ul>

          <p className="mt-4 rounded-xl bg-[var(--pivo-blue)]/12 px-3 py-2 text-sm text-slate-700">
            Most POS vendors require credentials and outlet mapping. A plain dashboard link is usually not enough for automated pull.
          </p>
        </section>
      </div>
    </section>
  );
}
