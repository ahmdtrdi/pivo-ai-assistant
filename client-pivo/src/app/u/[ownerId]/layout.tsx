import { SidebarNav } from "@/components/pivo/sidebar-nav";

type OwnerLayoutProps = {
  children: React.ReactNode;
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerLayout({ children, params }: OwnerLayoutProps) {
  const { ownerId } = await params;

  return (
    <main className="mx-auto w-full max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div className="grid gap-6 lg:grid-cols-[280px_1fr]">
        <div className="lg:sticky lg:top-6 lg:h-[calc(100vh-3rem)]">
          <SidebarNav ownerId={ownerId} />
        </div>
        <section className="min-w-0">{children}</section>
      </div>
    </main>
  );
}
