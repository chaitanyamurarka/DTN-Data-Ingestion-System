"use client";

import React from 'react';
import { Button, Tooltip, Spinner } from '@nextui-org/react';
import { Trash2 } from 'lucide-react';
import { IngestedSymbol } from '../lib/types';

interface IngestedSymbolsTableProps {
    symbols: IngestedSymbol[];
    isLoading: boolean;
    onSetSymbols: (symbols: IngestedSymbol[]) => void;
}

const IngestedSymbolsTable: React.FC<IngestedSymbolsTableProps> = ({ symbols, isLoading, onSetSymbols }) => {

    const handleRemoveSymbol = (symbolToRemove: IngestedSymbol) => {
        const newSymbols = symbols.filter(s => !(s.symbol === symbolToRemove.symbol && s.exchange === symbolToRemove.exchange));
        onSetSymbols(newSymbols);
    };

    return (
        <div className="overflow-x-auto">
            {/* The 'min-w-full' class has been removed below */}
            <table className="text-sm">
                <thead className="bg-slate-800">
                    <tr>
                        <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase">
                            Symbol
                        </th>
                        <th scope="col" className="text-left font-medium text-slate-400 px-6 py-3 uppercase">
                            Exchange
                        </th>
                        <th scope="col" className="text-right font-medium text-slate-400 px-6 py-3 uppercase">
                            Actions
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {isLoading && (
                        <tr>
                            <td colSpan={3} className="text-center p-8">
                                <Spinner label="Loading Symbols..." color="primary"/>
                            </td>
                        </tr>
                    )}
                    {!isLoading && (!symbols || symbols.length === 0) && (
                        <tr>
                            <td colSpan={3} className="text-center p-8 text-slate-500">
                                No symbols are currently being ingested.
                            </td>
                        </tr>
                    )}
                    {!isLoading && symbols && symbols.map((item) => (
                        <tr key={`${item.symbol}-${item.exchange}`} className="border-b border-slate-700 hover:bg-slate-700/50">
                            <td className="px-6 py-3 font-semibold text-slate-200">
                                {item.symbol}
                            </td>
                            <td className="px-6 py-3 text-slate-300">
                                {item.exchange}
                            </td>
                            <td className="px-6 py-3 text-right">
                                <Tooltip content="Remove symbol" color="danger">
                                    <Button
                                        isIconOnly
                                        color="danger"
                                        variant="light"
                                        onPress={() => handleRemoveSymbol(item)}
                                        aria-label="Remove"
                                    >
                                        <Trash2 size={18} />
                                    </Button>
                                </Tooltip>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};

export default IngestedSymbolsTable;