'use client';

import { useState, useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { Plus, RefreshCw } from 'lucide-react';
import SymbolSearch from '@/components/SymbolSearch';
import SymbolTable from '@/components/SymbolTable';
import AddSymbolModal from '@/components/AddSymbolModal';
import BulkUploadModal from '@/components/BulkUploadModal';
import { useSearchSymbols, useAddSymbol, useSetSymbols } from '@/hooks/useSymbols';
import { SearchParams, Symbol, SymbolUpdate } from '@/lib/types';
import toast from 'react-hot-toast';

export default function Home() {
  const [searchParams, setSearchParams] = useState<SearchParams>({});
  const [showAddModal, setShowAddModal] = useState(false);
  const [showBulkModal, setShowBulkModal] = useState(false);
  const [isInitialLoad, setIsInitialLoad] = useState(true);

  // Fetch all symbols on initial load
  const { data: symbols = [], isLoading, refetch, isRefetching } = useSearchSymbols(searchParams);
  const addSymbolMutation = useAddSymbol();
  const setSymbolsMutation = useSetSymbols();
  const queryClient = useQueryClient();

  // Load all symbols when component mounts
  useEffect(() => {
    if (isInitialLoad) {
      setIsInitialLoad(false);
      // Initial load with empty params to get all symbols
      setSearchParams({});
    }
  }, [isInitialLoad]);

  const handleAddSymbol = (symbol: SymbolUpdate) => {
    addSymbolMutation.mutate(symbol);
  };

  const handleAddFromTable = (symbol: Symbol) => {
    addSymbolMutation.mutate({
      symbol: symbol.symbol,
      exchange: symbol.exchange,
    });
  };

  const handleBulkUpload = (symbols: SymbolUpdate[]) => {
    setSymbolsMutation.mutate(symbols);
  };

  const handleRefresh = async () => {
    const { data } = await refetch();
    if (data) {
      toast.success(`Refreshed! Found ${data.length} symbols`);
    }
  };

  const totalSymbols = symbols.length;

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Symbol Admin Panel</h1>
              <p className="text-sm text-gray-600 mt-1">
                {totalSymbols > 0 ? `${totalSymbols} symbols available` : 'Loading symbols...'}
              </p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={handleRefresh}
                disabled={isRefetching}
                className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors flex items-center gap-2 disabled:opacity-50"
              >
                <RefreshCw size={20} className={isRefetching ? 'animate-spin' : ''} />
                Refresh
              </button>
              <button
                onClick={() => setShowAddModal(true)}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              >
                <Plus size={20} />
                Add Symbol
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="space-y-6">
          {/* Info Banner */}
          {!isLoading && symbols.length === 0 && (
            <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
              <p className="text-yellow-800">
                No symbols found. Try adjusting your search criteria or add symbols manually.
              </p>
            </div>
          )}

          <SymbolSearch onSearch={setSearchParams} />

          {isLoading && isInitialLoad ? (
            <div className="flex flex-col justify-center items-center h-64">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mb-4"></div>
              <p className="text-gray-600">Loading all available symbols...</p>
            </div>
          ) : (
            <SymbolTable
              symbols={symbols}
              onAddSymbol={handleAddFromTable}
              onBulkAction={() => setShowBulkModal(true)}
            />
          )}
        </div>
      </main>

      <AddSymbolModal
        isOpen={showAddModal}
        onClose={() => setShowAddModal(false)}
        onAdd={handleAddSymbol}
      />

      <BulkUploadModal
        isOpen={showBulkModal}
        onClose={() => setShowBulkModal(false)}
        onUpload={handleBulkUpload}
      />
    </div>
  );
}