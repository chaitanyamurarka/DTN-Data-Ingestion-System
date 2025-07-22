'use client';

import React, { useState, useMemo } from 'react';
import { Input, Button, Select, SelectItem, Card, CardBody, CardHeader, Divider } from '@nextui-org/react';
import { useSearchSymbols, useIngestedSymbols, useAddSymbol, useSetSymbols } from '../hooks/useSymbols';
import { IngestedSymbol, SymbolUpdate } from '../lib/types';
import SymbolTable from './SymbolTable';
import IngestedSymbolsTable from './IngestedSymbolsTable';
import AddSymbolModal from './AddSymbolModal';
import BulkUploadModal from './BulkUploadModal';
import toast from 'react-hot-toast';
import { useDebounce } from 'use-debounce';

const SymbolManagement: React.FC = () => {
  const [searchString, setSearchString] = useState('');
  const [debouncedSearchString] = useDebounce(searchString, 500);
  const [exchange, setExchange] = useState('');
  const [securityType, setSecurityType] = useState('');
  const [isAddModalOpen, setAddModalOpen] = useState(false);
  const [isBulkUploadModalOpen, setBulkUploadModalOpen] = useState(false);

  const searchEnabled = useMemo(() => debouncedSearchString.length > 2, [debouncedSearchString]);

  const { data: symbols, isLoading: isSearchLoading } = useSearchSymbols(
    { search_string: debouncedSearchString, exchange, security_type: securityType },
    searchEnabled
  );

  const { data: ingestedSymbols, isLoading: isIngestedLoading } = useIngestedSymbols();
  const addSymbolMutation = useAddSymbol();
  const setSymbolsMutation = useSetSymbols();


  const handleAddSymbol = (symbol: IngestedSymbol) => {
    toast.promise(
      addSymbolMutation.mutateAsync(symbol),
      {
        loading: `Adding ${symbol.symbol}...`,
        success: `${symbol.symbol} added successfully!`,
        error: `Error adding ${symbol.symbol}`,
      }
    );
  };

  const handleSetSymbols = (symbols: IngestedSymbol[]) => {
    toast.promise(
      setSymbolsMutation.mutateAsync(symbols),
      {
        loading: 'Updating ingested symbols...',
        success: 'Ingested symbols updated successfully!',
        error: 'Error updating ingested symbols',
      }
    );
  };

  const handleBulkUpload = (symbols: SymbolUpdate[]) => {
    if (ingestedSymbols) {
        const newSymbols = [...ingestedSymbols, ...symbols];
        const uniqueSymbols = Array.from(new Map(newSymbols.map(item => [`${item.symbol}-${item.exchange}`, item])).values());
        handleSetSymbols(uniqueSymbols);
    } else {
        handleSetSymbols(symbols);
    }
  };


  return (
    <>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <Card>
          <CardHeader>
            <h2 className="text-2xl font-bold">Search Symbols</h2>
          </CardHeader>
          <Divider />
          <CardBody>
            <div className="flex flex-col md:flex-row gap-4 mb-4">
              <Input
                isClearable
                aria-label="Search Symbol"
                placeholder="Search for a symbol (e.g., AAPL, NQ)"
                value={searchString}
                onValueChange={setSearchString}
                className="w-full"
              />
              <Select
                aria-label="Exchange"
                placeholder="Exchange"
                selectedKeys={exchange ? [exchange] : []}
                onChange={(e) => setExchange(e.target.value)}
                className="w-full md:w-1/2"
              >
                <SelectItem key="NASDAQ" value="NASDAQ">NASDAQ</SelectItem>
                <SelectItem key="NYSE" value="NYSE">NYSE</SelectItem>
                <SelectItem key="CME" value="CME">CME</SelectItem>
              </Select>
              <Select
                aria-label="Security Type"
                placeholder="Security Type"
                selectedKeys={securityType ? [securityType] : []}
                onChange={(e) => setSecurityType(e.target.value)}
                className="w-full md:w-1/2"
              >
                <SelectItem key="STOCK" value="STOCK">Stock</SelectItem>
                <SelectItem key="FUTURES" value="FUTURES">Futures</SelectItem>
              </Select>
            </div>
            <SymbolTable
              symbols={symbols || []}
              onAddSymbol={handleAddSymbol}
              isLoading={isSearchLoading}
              onBulkAction={() => setBulkUploadModalOpen(true)}
            />
          </CardBody>
        </Card>
        <Card>
          <CardHeader>
            <div className="flex justify-between items-center w-full">
              <h2 className="text-2xl font-bold">Ingested Symbols</h2>
              <Button color="primary" onPress={() => setAddModalOpen(true)}>
                Add Symbol
              </Button>
            </div>
          </CardHeader>
          <Divider />
          <CardBody>
            <IngestedSymbolsTable
              symbols={ingestedSymbols || []}
              isLoading={isIngestedLoading}
              onSetSymbols={handleSetSymbols}
            />
          </CardBody>
        </Card>
      </div>
      <AddSymbolModal
        isOpen={isAddModalOpen}
        onClose={() => setAddModalOpen(false)}
        onAdd={handleAddSymbol}
      />
      <BulkUploadModal
        isOpen={isBulkUploadModalOpen}
        onClose={() => setBulkUploadModalOpen(false)}
        onUpload={handleBulkUpload}
      />
    </>
  );
};

export default SymbolManagement;