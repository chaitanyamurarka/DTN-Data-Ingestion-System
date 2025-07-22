"use client";

import React from 'react';
import { useIngestedSymbols } from '../hooks/useSymbols';
import {
  Table,
  TableHeader,
  TableColumn,
  TableBody,
  TableRow,
  TableCell,
  Spinner,
} from '@nextui-org/react';

const IngestedSymbolsTable: React.FC = () => {
  const { data: ingestedSymbols, isLoading, isError } = useIngestedSymbols();

  return (
    <div className="mt-8">
      <h2 className="text-2xl font-bold mb-4">Currently Ingested Symbols</h2>
      {isLoading ? (
        <div className="flex justify-center items-center">
          <Spinner label="Loading Symbols..." />
        </div>
      ) : isError ? (
        <p className="text-danger">Error loading symbols.</p>
      ) : (
        <Table aria-label="Table of ingested symbols">
          <TableHeader>
            <TableColumn>SYMBOL</TableColumn>
            <TableColumn>EXCHANGE</TableColumn>
          </TableHeader>
          <TableBody
            items={ingestedSymbols || []}
            emptyContent={'No symbols are currently being ingested.'}
          >
            {(item) => (
              <TableRow key={`${item.symbol}-${item.exchange}`}>
                <TableCell>{item.symbol}</TableCell>
                <TableCell>{item.exchange}</TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      )}
    </div>
  );
};

export default IngestedSymbolsTable;