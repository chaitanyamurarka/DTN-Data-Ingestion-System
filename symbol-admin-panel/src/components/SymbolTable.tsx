'use client';

import { useState } from 'react';
import { Plus, Trash2, Upload, CheckCircle } from 'lucide-react';
import { Symbol } from '@/lib/types';

interface SymbolTableProps {
  symbols: Symbol[];
  onAddSymbol: (symbol: Symbol) => void;
  onDeleteSymbol?: (symbol: Symbol) => void;
  onBulkAction?: () => void;
}

const securityTypeColors: Record<string, string> = {
  EQUITY: 'bg-green-100 text-green-800',
  INDEX: 'bg-blue-100 text-blue-800',
  MONEY: 'bg-yellow-100 text-yellow-800',
  BONDS: 'bg-purple-100 text-purple-800',
  FUTURE: 'bg-orange-100 text-orange-800',
  FOPTION: 'bg-pink-100 text-pink-800',
};

export default function SymbolTable({
  symbols,
  onAddSymbol,
  onDeleteSymbol,
  onBulkAction,
}: SymbolTableProps) {
  const [selectedSymbols, setSelectedSymbols] = useState<Set<string>>(new Set());
  const [addedSymbols, setAddedSymbols] = useState<Set<string>>(new Set());

  const toggleSymbolSelection = (symbolKey: string) => {
    const newSelection = new Set(selectedSymbols);
    if (newSelection.has(symbolKey)) {
      newSelection.delete(symbolKey);
    } else {
      newSelection.add(symbolKey);
    }
    setSelectedSymbols(newSelection);
  };

  const toggleAllSelection = () => {
    if (selectedSymbols.size === symbols.length) {
      setSelectedSymbols(new Set());
    } else {
      setSelectedSymbols(new Set(symbols.map(s => `${s.symbol}-${s.exchange}`)));
    }
  };

  const handleAddSymbol = (symbol: Symbol) => {
    onAddSymbol(symbol);
    // Mark symbol as added for visual feedback
    setAddedSymbols(prev => new Set(prev).add(`${symbol.symbol}-${symbol.exchange}`));
    // Remove the visual feedback after 3 seconds
    setTimeout(() => {
      setAddedSymbols(prev => {
        const newSet = new Set(prev);
        newSet.delete(`${symbol.symbol}-${symbol.exchange}`);
        return newSet;
      });
    }, 3000);
  };

  const getSecurityTypeColor = (type?: string) => {
    if (!type) return 'bg-gray-100 text-gray-800';
    return securityTypeColors[type] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="bg-white rounded-lg shadow-md overflow-hidden">
      <div className="p-4 border-b flex justify-between items-center">
        <h2 className="text-lg font-semibold">Available Symbols ({symbols.length})</h2>
        <div className="flex gap-2">
          {selectedSymbols.size > 0 && (
            <button
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors flex items-center gap-2"
            >
              <Trash2 size={16} />
              Delete Selected ({selectedSymbols.size})
            </button>
          )}
          {onBulkAction && (
            <button
              onClick={onBulkAction}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center gap-2"
            >
              <Upload size={16} />
              Bulk Upload
            </button>
          )}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left">
                <input
                  type="checkbox"
                  checked={selectedSymbols.size === symbols.length && symbols.length > 0}
                  onChange={toggleAllSelection}
                  className="rounded"
                />
              </th>
              <th className="px-4 py-3 text-left text-sm font-medium text-gray-700">
                Symbol
              </th>
              <th className="px-4 py-3 text-left text-sm font-medium text-gray-700">
                Exchange
              </th>
              <th className="px-4 py-3 text-left text-sm font-medium text-gray-700">
                Description
              </th>
              <th className="px-4 py-3 text-left text-sm font-medium text-gray-700">
                Type
              </th>
              <th className="px-4 py-3 text-left text-sm font-medium text-gray-700">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y">
            {symbols.map((symbol) => {
              const key = `${symbol.symbol}-${symbol.exchange}`;
              const isAdded = addedSymbols.has(key);
              return (
                <tr key={key} className={`hover:bg-gray-50 transition-colors ${isAdded ? 'bg-green-50' : ''}`}>
                  <td className="px-4 py-3">
                    <input
                      type="checkbox"
                      checked={selectedSymbols.has(key)}
                      onChange={() => toggleSymbolSelection(key)}
                      className="rounded"
                    />
                  </td>
                  <td className="px-4 py-3 font-medium">{symbol.symbol}</td>
                  <td className="px-4 py-3">{symbol.exchange}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {symbol.description || '-'}
                  </td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-1 text-xs rounded ${getSecurityTypeColor(symbol.securityType)}`}>
                      {symbol.securityType || 'N/A'}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => handleAddSymbol(symbol)}
                      className={`flex items-center gap-1 transition-all ${
                        isAdded 
                          ? 'text-green-600' 
                          : 'text-blue-600 hover:text-blue-800'
                      }`}
                      title={isAdded ? 'Added to ingestion' : 'Add to ingestion'}
                    >
                      {isAdded ? (
                        <>
                          <CheckCircle size={16} />
                          <span className="text-sm">Added</span>
                        </>
                      ) : (
                        <>
                          <Plus size={16} />
                          <span className="text-sm">Add</span>
                        </>
                      )}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {symbols.length === 0 && (
        <div className="p-8 text-center text-gray-500">
          <p className="mb-2">No symbols available.</p>
          <p className="text-sm">Symbols will appear here once they are loaded from the backend.</p>
        </div>
      )}
    </div>
  );
}