'use client';

import { useState } from 'react';
import { X } from 'lucide-react';
import { SymbolUpdate } from '@/lib/types';

interface AddSymbolModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAdd: (symbol: SymbolUpdate) => void;
}

export default function AddSymbolModal({ isOpen, onClose, onAdd }: AddSymbolModalProps) {
  const [symbol, setSymbol] = useState('');
  const [exchange, setExchange] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (symbol && exchange) {
      onAdd({ symbol: symbol.toUpperCase(), exchange });
      setSymbol('');
      setExchange('');
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-full max-w-md">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Add Symbol for Ingestion</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600"
          >
            <X size={24} />
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Symbol
            </label>
            <input
              type="text"
              value={symbol}
              onChange={(e) => setSymbol(e.target.value)}
              className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="e.g., AAPL"
              required
            />
          </div>

          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Exchange
            </label>
            <select
              value={exchange}
              onChange={(e) => setExchange(e.target.value)}
              className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
            >
              <option value="">Select Exchange</option>
              <option value="NYSE">NYSE</option>
              <option value="NASDAQ">NASDAQ</option>
              <option value="CME">CME</option>
              <option value="AMEX">AMEX</option>
            </select>
          </div>

          <div className="flex gap-3">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              Add Symbol
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}