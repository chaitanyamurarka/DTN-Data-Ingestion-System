"use client";

import React from 'react';
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
import { Trash2, X } from 'lucide-react';
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

    const handleClearAll = () => {
        onSetSymbols([]);
    };

  return (
    <div className="mt-8">
        <div className="flex justify-between items-center mb-4">
            <h2 className="text-2xl font-bold">Currently Ingested Symbols ({symbols.length})</h2>
            <Button color="danger" variant="flat" onPress={handleClearAll} startContent={<X size={16}/>}>
                Clear All
            </Button>
        </div>
        <Table aria-label="Table of ingested symbols">
            <TableHeader>
                <TableColumn>SYMBOL</TableColumn>
                <TableColumn>EXCHANGE</TableColumn>
                <TableColumn>ACTIONS</TableColumn>
            </TableHeader>
            <TableBody
                items={symbols}
                isLoading={isLoading}
                loadingContent={<Spinner label="Loading Symbols..." />}
                emptyContent={'No symbols are currently being ingested.'}
            >
                {(item) => (
                <TableRow key={`${item.symbol}-${item.exchange}`}>
                    <TableCell>{item.symbol}</TableCell>
                    <TableCell>{item.exchange}</TableCell>
                    <TableCell>
                        <Tooltip content="Remove symbol">
                            <Button
                                isIconOnly
                                color="danger"
                                variant="light"
                                onPress={() => handleRemoveSymbol(item)}
                            >
                                <Trash2 size={16} />
                            </Button>
                        </Tooltip>
                    </TableCell>
                </TableRow>
                )}
            </TableBody>
        </Table>
    </div>
  );
};

export default IngestedSymbolsTable;