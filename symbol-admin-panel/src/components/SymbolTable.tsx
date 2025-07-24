'use client';

import { Button, Tooltip, Spinner } from '@nextui-org/react';
import { Plus, CheckCircle } from 'lucide-react';
import { Symbol as SymbolType, IngestedSymbol } from '@/lib/types';
import React from 'react';

interface SymbolTableProps {
  symbols: SymbolType[];
  onAddSymbol: (symbol: IngestedSymbol) => void;
  isLoading: boolean;
}

const securityTypeColors: Record<string, string> = {
  STOCK: 'bg-green-900/50 text-green-300 border border-green-700',
  FUTURES: 'bg-orange-900/50 text-orange-300 border border-orange-700',
};

const getSecurityTypeColor = (type?: string) => {
  if (!type) return 'bg-slate-700 text-slate-300';
  return securityTypeColors[type.toUpperCase()] || 'bg-slate-700 text-slate-300';
};

export default function SymbolTable({
  symbols,
  onAddSymbol,
  isLoading,
}: SymbolTableProps) {
  const [addedSymbols, setAddedSymbols] = React.useState<Set<string>>(new Set());

  const handleAddSymbol = (symbol: SymbolType) => {
    onAddSymbol({ symbol: symbol.symbol, exchange: symbol.exchange });
    setAddedSymbols(prev => new Set(prev).add(`${symbol.symbol}-${symbol.exchange}`));
  };

  return (
    <div className="overflow-x-auto">
        {/* The 'min-w-full' class has been removed below */}
        <table className="text-sm">
            <thead className="bg-slate-800">
                <tr>
                    <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase">Symbol</th>
                    <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase w-2/5">Description</th>
                    <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase">Exchange</th>
                    <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase">Type</th>
                    <th scope="col" className="text-right font-medium text-slate-400 px-6 py-3 uppercase">Actions</th>
                </tr>
            </thead>
            <tbody>
                {isLoading && (
                    <tr>
                        <td colSpan={5} className="text-center p-8">
                            <Spinner label="Searching..." color="primary" />
                        </td>
                    </tr>
                )}
                {!isLoading && (!symbols || symbols.length === 0) && (
                    <tr>
                        <td colSpan={5} className="text-center p-8 text-slate-500">
                             Type 3+ characters in the search bar to begin.
                        </td>
                    </tr>
                )}
                {!isLoading && symbols && symbols.map((item) => {
                    const key = `${item.symbol}-${item.exchange}`;
                    const isAdded = addedSymbols.has(key);
                    return (
                        <tr key={key} className="border-b border-slate-700 hover:bg-slate-700/50">
                            <td className="px-6 py-3 font-semibold text-slate-200">{item.symbol}</td>
                            <td className="px-6 py-3 text-slate-400">{item.description}</td>
                            <td className="px-6 py-3 text-slate-300">{item.exchange}</td>
                            <td className="px-6 py-3">
                                <span className={`px-2.5 py-1 text-xs rounded-full ${getSecurityTypeColor(item.securityType)}`}>
                                {item.securityType || 'N/A'}
                                </span>
                            </td>
                            <td className="px-6 py-3 text-right">
                                <Tooltip content={isAdded ? "Symbol has been added" : "Add to ingestion list"} color="primary">
                                <Button
                                    isIconOnly
                                    color={isAdded ? "success" : "primary"}
                                    variant={isAdded ? "flat" : "solid"}
                                    onPress={() => handleAddSymbol(item)}
                                    aria-label={isAdded ? "Added" : "Add"}
                                >
                                    {isAdded ? <CheckCircle size={18} /> : <Plus size={18} />}
                                </Button>
                                </Tooltip>
                            </td>
                        </tr>
                    );
                })}
            </tbody>
        </table>
    </div>
  );
}