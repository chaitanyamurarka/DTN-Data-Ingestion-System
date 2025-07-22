'use client';

import {
  Table,
  TableHeader,
  TableColumn,
  TableBody,
  TableRow,
  TableCell,
  Spinner,
  Button,
  Tooltip,
} from '@nextui-org/react';
import { Plus, CheckCircle, Upload } from 'lucide-react';
import { Symbol as SymbolType, IngestedSymbol } from '@/lib/types';
import React from 'react';

interface SymbolTableProps {
  symbols: SymbolType[];
  onAddSymbol: (symbol: IngestedSymbol) => void;
  isLoading: boolean;
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

const getSecurityTypeColor = (type?: string) => {
  if (!type) return 'bg-gray-100 text-gray-800';
  return securityTypeColors[type] || 'bg-gray-100 text-gray-800';
};

export default function SymbolTable({
  symbols,
  onAddSymbol,
  isLoading,
  onBulkAction
}: SymbolTableProps) {

  const [addedSymbols, setAddedSymbols] = React.useState<Set<string>>(new Set());

  const handleAddSymbol = (symbol: SymbolType) => {
    onAddSymbol({ symbol: symbol.symbol, exchange: symbol.exchange });
    setAddedSymbols(prev => new Set(prev).add(`${symbol.symbol}-${symbol.exchange}`));
  };

  return (
    <div className="bg-white rounded-lg shadow-md overflow-hidden">
        <div className="p-4 border-b flex justify-between items-center">
            <h2 className="text-lg font-semibold">Available Symbols ({symbols.length})</h2>
            {onBulkAction && (
                <Button color="secondary" onPress={onBulkAction} startContent={<Upload size={16} />}>
                Bulk Upload
                </Button>
            )}
        </div>
        <Table aria-label="Table of available symbols">
            <TableHeader>
                <TableColumn>SYMBOL</TableColumn>
                <TableColumn>DESCRIPTION</TableColumn>
                <TableColumn>EXCHANGE</TableColumn>
                <TableColumn>TYPE</TableColumn>
                <TableColumn>ACTIONS</TableColumn>
            </TableHeader>
            <TableBody
                items={symbols}
                isLoading={isLoading}
                loadingContent={<Spinner label="Loading..." />}
                emptyContent={!isLoading ? 'No symbols found. Type in the search bar to begin.' : ' '}
            >
                {(item) => {
                    const key = `${item.symbol}-${item.exchange}`;
                    const isAdded = addedSymbols.has(key);
                    return (
                        <TableRow key={key}>
                            <TableCell>{item.symbol}</TableCell>
                            <TableCell>{item.description}</TableCell>
                            <TableCell>{item.exchange}</TableCell>
                            <TableCell>
                                <span className={`px-2 py-1 text-xs rounded ${getSecurityTypeColor(item.securityType)}`}>
                                    {item.securityType || 'N/A'}
                                </span>
                            </TableCell>
                            <TableCell>
                                <Tooltip content={isAdded ? "Symbol has been added" : "Add to ingestion list"}>
                                    <Button
                                        isIconOnly
                                        color={isAdded ? "success" : "primary"}
                                        variant="flat"
                                        onPress={() => handleAddSymbol(item)}
                                    >
                                        {isAdded ? <CheckCircle size={16} /> : <Plus size={16} />}
                                    </Button>
                                </Tooltip>
                            </TableCell>
                        </TableRow>
                    )
                }}
            </TableBody>
        </Table>
    </div>
  );
}