'use client';

import { useState } from 'react';
import { Search, Filter, X } from 'lucide-react';
import { SearchParams } from '@/lib/types';

interface SymbolSearchProps {
  onSearch: (params: SearchParams) => void;
}

export default function SymbolSearch({ onSearch }: SymbolSearchProps) {
  const [searchString, setSearchString] = useState('');
  const [exchange, setExchange] = useState('');
  const [securityType, setSecurityType] = useState('');
  const [showFilters, setShowFilters] = useState(false);

  const handleSearch = () => {
    const params: SearchParams = {};
    if (searchString) params.search_string = searchString;
    if (exchange) params.exchange = exchange;
    if (securityType) params.security_type = securityType;
    onSearch(params);
  };

  const handleClear = () => {
    setSearchString('');
    setExchange('');
    setSecurityType('');
    onSearch({}); // Load all symbols
  };

  const hasActiveFilters = searchString || exchange || securityType;

  return (
    <div className="bg-white p-4 rounded-lg shadow-md">
      <div className="flex gap-2 mb-4">
        <div className="relative flex-1">
          <input
            type="text"
            placeholder="Search symbols or descriptions..."
            value={searchString}
            onChange={(e) => setSearchString(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
            className="w-full pl-10 pr-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <Search className="absolute left-3 top-2.5 text-gray-400" size={20} />
        </div>
        <button
          onClick={() => setShowFilters(!showFilters)}
          className={`px-4 py-2 rounded-lg transition-colors ${
            showFilters ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 hover:bg-gray-200'
          }`}
        >
          <Filter size={20} />
        </button>
        <button
          onClick={handleSearch}
          className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          Search
        </button>
        {hasActiveFilters && (
          <button
            onClick={handleClear}
            className="px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors flex items-center gap-1"
          >
            <X size={16} />
            Clear
          </button>
        )}
      </div>

      {showFilters && (
        <div className="grid grid-cols-2 gap-4 pt-4 border-t">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Exchange
            </label>
            <select
              value={exchange}
              onChange={(e) => setExchange(e.target.value)}
              className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All Exchanges</option>
              <option value="NYSE">NYSE</option>
              <option value="NASDAQ">NASDAQ</option>
              <option value="CME">CME</option>
              <option value="AMEX">AMEX</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Security Type
            </label>
            <select
              value={securityType}
              onChange={(e) => setSecurityType(e.target.value)}
              className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All Types</option>
              <option value="EQUITY">Equity</option>
              <option value="INDEX">Index</option>
              <option value="MONEY">Money</option>
              <option value="BONDS">Bonds</option>
              <option value="FUTURE">Future</option>
              <option value="FOPTION">FOption</option>
            </select>
          </div>
        </div>
      )}

      {hasActiveFilters && (
        <div className="mt-3 text-sm text-gray-600">
          Filtering results
          {searchString && <span className="font-medium"> containing "{searchString}"</span>}
          {exchange && <span className="font-medium"> on {exchange}</span>}
          {securityType && <span className="font-medium"> of type {securityType}</span>}
        </div>
      )}
    </div>
  );
}