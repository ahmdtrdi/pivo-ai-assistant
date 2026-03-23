import Link from "next/link";

export default function HomePage() {
  return (
    <main className="mx-auto flex min-h-[calc(100vh-5rem)] w-full max-w-5xl flex-col justify-center px-4 py-10 sm:px-6 lg:px-8">
      <section className="rounded-3xl border border-[var(--pivo-blue)]/20 bg-gradient-to-br from-white via-[var(--pivo-primary)] to-[var(--pivo-blue)]/10 p-6 shadow-sm sm:p-10">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-[var(--pivo-blue)]">PIVO</p>
        <h1 className="mt-3 text-3xl font-bold leading-tight text-slate-900 sm:text-5xl">From Data to Daily Decisions</h1>
        <p className="mt-4 max-w-2xl text-sm leading-relaxed text-slate-700 sm:text-base">
          Dashboard dan Supply Simulator sudah siap dipakai untuk tahap pertama implementasi frontend.
          Buka halaman owner untuk membaca payload harian dari Supabase.
        </p>

        <div className="mt-6 flex flex-wrap gap-3">
          <Link
            href="/u/demo"
            className="rounded-xl bg-[var(--pivo-navy)] px-4 py-2 text-sm font-semibold text-white transition hover:bg-[var(--pivo-blue)]"
          >
            Open Demo Owner
          </Link>
          <Link
            href="/u/demo/history"
            className="rounded-xl border border-[var(--pivo-navy)]/30 bg-white px-4 py-2 text-sm font-semibold text-[var(--pivo-navy)] transition hover:bg-[var(--pivo-primary)]"
          >
            Open Sales History
          </Link>
          <span className="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm text-slate-600">Route format: /u/&lt;owner_id&gt;</span>
        </div>
      </section>
    </main>
  );
}
