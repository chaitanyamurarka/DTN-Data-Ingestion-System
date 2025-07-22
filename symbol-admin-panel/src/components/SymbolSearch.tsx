"use client";

import React, { useState } from 'react';
import { Input, Button, Select, SelectItem } from '@nextui-org/react';
import { useSearchSymbols, useAddSymbol } from '../hooks/useSymbols';
import { Symbol as SymbolType } from '../lib/types';

const SymbolSearch: React.FC = () => {
  const [searchString, setSearchString] = useState('');
  const [exchange, setExchange] = useState('');
  const [securityType, setSecurityType] = useState('');

  const { data: symbols, isLoading } = useSearchSymbols({
    search_string: searchString,
    exchange,
    security_type: securityType,
  });
  const addSymbolMutation = useAddSymbol();

  const handleAddSymbol = (symbol: SymbolType) => {
    addSymbolMutation.mutate(
      { symbol: symbol.symbol, exchange: symbol.exchange },
      {
        onSuccess: () => {
          alert(`Symbol ${symbol.symbol} added successfully!`);
        },
        onError: (error) => {
          alert(`Error adding symbol: ${error.message}`);
        },
      }
    );
  };

  return (
    <div className="w-full">
      <div className="flex flex-col md:flex-row gap-4 mb-4">
        <Input
          isClearable
          placeholder="Search for a symbol (e.g., AAPL, NQ)"
          value={searchString}
          onValueChange={setSearchString}
          className="w-full md:w-1/2"
        />
        <Select
          placeholder="Select Exchange"
          selectedKeys={exchange ? [exchange] : []}
          onChange={(e) => setExchange(e.target.value)}
          className="w-full md:w-1/4"
        >
          <SelectItem key="NASDAQ" value="NASDAQ">
            NASDAQ
          </SelectItem>
          <SelectItem key="NYSE" value="NYSE">
            NYSE
          </SelectItem>
          <SelectItem key="CME" value="CME">
            CME
          </SelectItem>
        </Select>
        <Select
          placeholder="Select Security Type"
          selectedKeys={securityType ? [securityType] : []}
          onChange={(e) => setSecurityType(e.target.value)}
          className="w-full md:w-1/4"
        >
          <SelectItem key="STOCK" value="STOCK">
            Stock
          </SelectItem>
          <SelectItem key="FUTURES" value="FUTURES">
            Futures
          </SelectItem>
        </Select>
      </div>

      {isLoading && <p>Loading...</p>}
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {symbols?.map((symbol) => (
          <div key={`${symbol.symbol}-${symbol.exchange}`} className="p-4 border rounded-lg shadow-md bg-white dark:bg-gray-800">
            <h3 className="font-bold text-lg">{symbol.symbol}</h3>
            <p className="text-gray-600 dark:text-gray-400">{symbol.description}</p>
            <div className="flex justify-between items-center mt-2">
              <span className="text-sm bg-gray-200 dark:bg-gray-700 px-2 py-1 rounded">
                {symbol.exchange} - {symbol.securityType}
              </span>
              <Button
                color="primary"
                onClick={() => handleAddSymbol(symbol)}
                disabled={addSymbolMutation.isPending}
              >
                {addSymbolMutation.isPending ? 'Adding...' : 'Add'}
              </Button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default SymbolSearch;