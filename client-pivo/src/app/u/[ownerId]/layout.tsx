import { OwnerShell } from "@/components/pivo/owner-shell";

type OwnerLayoutProps = {
  children: React.ReactNode;
  params: Promise<{ ownerId: string }>;
};

export default async function OwnerLayout({ children, params }: OwnerLayoutProps) {
  const { ownerId } = await params;

  return (
    <main className="mx-auto w-full max-w-[1680px] px-2 py-3 sm:px-3 lg:px-4">
      <OwnerShell ownerId={ownerId}>{children}</OwnerShell>
    </main>
  );
}
