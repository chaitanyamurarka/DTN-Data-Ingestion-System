'use client';

import React, { useState, useMemo } from 'react';
import { Input, Button, Select, SelectItem, Card, CardBody, Divider } from '@nextui-org/react';
import { useSearchSymbols, useIngestedSymbols, useAddSymbol, useSetSymbols } from '../hooks/useSymbols';
import { IngestedSymbol, SymbolUpdate } from '../lib/types';
import SymbolTable from './SymbolTable';
import IngestedSymbolsTable from './IngestedSymbolsTable';
import AddSymbolModal from './AddSymbolModal';
import BulkUploadModal from './BulkUploadModal';
import toast from 'react-hot-toast';
import { useDebounce } from 'use-debounce';
import { Search, Plus, Upload, DatabaseZap } from 'lucide-react';

const SymbolManagement: React.FC = () => {
  const [searchString, setSearchString] = useState('');
  const [debouncedSearchString] = useDebounce(searchString, 500);
  const [exchange, setExchange] = useState<string>('');
  const [securityType, setSecurityType] = useState<string>('');
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
    const currentSymbols = ingestedSymbols || [];
    const newSymbols = [...currentSymbols, ...symbols];
    const uniqueSymbolsMap = new Map(newSymbols.map(item => [`${item.symbol}-${item.exchange}`, item]));
    const uniqueSymbols = Array.from(uniqueSymbolsMap.values());
    handleSetSymbols(uniqueSymbols);
    toast.success(`${symbols.length} symbols processed for bulk upload!`);
  };

  return (
    <>
      {/* Main Content Area */}
      <div className="flex flex-col gap-8">
        {/* Search and Results Section */}
        <Card className="bg-slate-800/60 backdrop-blur-xl border border-slate-700/80 shadow-lg">
          <CardBody className="p-6 space-y-6">
            <div>
                <h2 className="text-2xl font-semibold text-slate-100 flex items-center gap-3">
                    <Search className="text-blue-400" />
                    Symbol Discovery
                </h2>
                <p className="text-slate-400 text-sm mt-1">Find new symbols by name, description, exchange, or type.</p>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Input
                isClearable
                aria-label="Search Symbol"
                placeholder="Search by symbol or description (3+ chars)..."
                value={searchString}
                onValueChange={setSearchString}
                className="md:col-span-2"
                classNames={{
                  inputWrapper: [
                      "bg-slate-900/50",
                      "border",
                      "border-slate-700",
                      "group-data-[focus=true]:border-blue-500",
                      "group-data-[focus=true]:ring-2",
                      "group-data-[focus=true]:ring-blue-500/20",
                      "shadow-inner",
                      "hover:bg-slate-800",
                      "transition-all"
                  ],
                  input: "text-slate-200",
                }}
              />
              <div className="grid grid-cols-2 gap-4">
                <Select
                    aria-label="Exchange"
                    placeholder="Exchange"
                    selectedKeys={exchange ? [exchange] : []}
                    onChange={(e) => setExchange(e.target.value)}
                >
                    <SelectItem key="" value="">All Exchanges</SelectItem>
                    <SelectItem key="NASDAQ" value="NASDAQ">NASDAQ</SelectItem>
                    <SelectItem key="NYSE" value="NYSE">NYSE</SelectItem>
                    <SelectItem key="CME" value="CME">CME</SelectItem>
                </Select>
                <Select
                    aria-label="Security Type"
                    placeholder="Type"
                    selectedKeys={securityType ? [securityType] : []}
                    onChange={(e) => setSecurityType(e.target.value)}
                >
                    <SelectItem key="" value="">All Types</SelectItem>
                    <SelectItem key="STOCK" value="STOCK">Stock</SelectItem>
                    <SelectItem key="FUTURES" value="FUTURES">Futures</SelectItem>
                </Select>
              </div>
            </div>
            <SymbolTable
              symbols={symbols || []}
              onAddSymbol={handleAddSymbol}
              isLoading={isSearchLoading}
            />
          </CardBody>
        </Card>
        
        {/* Ingested Symbols Section */}
        <Card className="bg-slate-800/60 backdrop-blur-xl border border-slate-700/80 shadow-lg">
          <CardBody className="p-6 space-y-4">
            <div className="flex flex-wrap gap-4 justify-between items-center">
                <div>
                    <h2 className="text-2xl font-semibold text-slate-100 flex items-center gap-3">
                        <DatabaseZap className="text-blue-400"/>
                        Ingestion Management
                    </h2>
                    <p className="text-slate-400 text-sm mt-1">Symbols currently being ingested by the system.</p>
                </div>
                <div className="flex gap-4">
                    <Button color="primary" variant="ghost" onPress={() => setAddModalOpen(true)} startContent={<Plus size={16} />}>
                        Add Manually
                    </Button>
                    <Button color="primary" variant="solid" onPress={() => setBulkUploadModalOpen(true)} startContent={<Upload size={16} />}>
                        Bulk Upload
                    </Button>
                </div>
            </div>
            <Divider className="my-2 bg-slate-700"/>
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