import SymbolManagement from "@/components/SymbolManagement";
import { BarChart2 } from 'lucide-react';

export default function Home() {
  return (
    <div className="min-h-screen text-slate-200">
      <main className="container mx-auto p-4 sm:p-6 lg:p-8">
        <div className="text-center mb-12">
            <div className="flex justify-center items-center gap-4">
              <div className="p-3 bg-white/10 rounded-lg">
                <BarChart2 size={32} className="text-cyan-300"/>
              </div>
              <h1 className="text-5xl font-bold text-white tracking-tight">
                Symbol Admin Panel
              </h1>
            </div>
            <p className="text-slate-400 mt-4 max-w-2xl mx-auto">
              A centralized dashboard to search for new financial symbols and manage the active data ingestion pipeline.
            </p>
        </div>
        <SymbolManagement />
      </main>
    </div>
  );
}