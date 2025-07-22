import SymbolSearch from "@/components/SymbolSearch";
import IngestedSymbolsTable from "@/components/IngestedSymbolsTable"; // Import the new component

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col items-center p-8 md:p-12 lg:p-24 bg-gray-50 dark:bg-gray-900">
      <div className="z-10 w-full max-w-7xl items-center justify-between font-mono text-sm lg:flex mb-8">
        <p className="fixed left-0 top-0 flex w-full justify-center border-b border-gray-300 bg-gradient-to-b from-zinc-200 pb-6 pt-8 backdrop-blur-2xl dark:border-neutral-800 dark:bg-zinc-800/30 dark:from-inherit lg:static lg:w-auto  lg:rounded-xl lg:border lg:bg-gray-200 lg:p-4 lg:dark:bg-zinc-800/30">
          DTN Data Ingestion&nbsp;
          <code className="font-mono font-bold">Admin Panel</code>
        </p>
      </div>

      <div className="w-full max-w-7xl">
        <SymbolSearch />
        <IngestedSymbolsTable /> {/* Add the new table here */}
      </div>

    </main>
  );
}