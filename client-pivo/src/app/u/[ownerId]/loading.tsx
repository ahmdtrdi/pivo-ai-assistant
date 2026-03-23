export default function LoadingOwnerDashboard() {
  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 lg:px-8">
      <div className="animate-pulse space-y-4">
        <div className="h-40 rounded-3xl bg-slate-200" />
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="h-36 rounded-2xl bg-slate-200" />
          <div className="h-36 rounded-2xl bg-slate-200" />
          <div className="h-36 rounded-2xl bg-slate-200" />
          <div className="h-36 rounded-2xl bg-slate-200" />
        </div>
      </div>
    </main>
  );
}
