'use client';

import { useState } from 'react';
import { X, Upload, FileText } from 'lucide-react';
import { SymbolUpdate } from '@/lib/types';

interface BulkUploadModalProps {
  isOpen: boolean;
  onClose: () => void;
  onUpload: (symbols: SymbolUpdate[]) => void;
}

export default function BulkUploadModal({ isOpen, onClose, onUpload }: BulkUploadModalProps) {
  const [textInput, setTextInput] = useState('');
  const [fileContent, setFileContent] = useState('');

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (event) => {
        setFileContent(event.target?.result as string);
      };
      reader.readAsText(file);
    }
  };

  const parseSymbols = (text: string): SymbolUpdate[] => {
    const lines = text.trim().split('\n');
    const symbols: SymbolUpdate[] = [];

    lines.forEach((line) => {
      const [symbol, exchange] = line.split(',').map(s => s.trim());
      if (symbol && exchange) {
        symbols.push({ symbol: symbol.toUpperCase(), exchange });
      }
    });

    return symbols;
  };

  const handleSubmit = () => {
    const content = fileContent || textInput;
    const symbols = parseSymbols(content);
    if (symbols.length > 0) {
      onUpload(symbols);
      setTextInput('');
      setFileContent('');
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[80vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Bulk Upload Symbols</h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600"
          >
            <X size={24} />
          </button>
        </div>

        <div className="mb-6">
          <p className="text-sm text-gray-600 mb-4">
            Upload a CSV file or paste symbols in the format: SYMBOL,EXCHANGE (one per line)
          </p>

          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Upload CSV File
            </label>
            <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center">
              <FileText className="mx-auto text-gray-400 mb-2" size={48} />
              <input
                type="file"
                accept=".csv,.txt"
                onChange={handleFileUpload}
                className="hidden"
                id="file-upload"
              />
              <label
                htmlFor="file-upload"
                className="cursor-pointer text-blue-600 hover:text-blue-700"
              >
                Click to upload file
              </label>
              {fileContent && (
                <p className="mt-2 text-sm text-green-600">
                  File loaded successfully
                </p>
              )}
            </div>
          </div>

          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Or Paste Symbols
            </label>
            <textarea
              value={textInput}
              onChange={(e) => setTextInput(e.target.value)}
              className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              rows={10}
              placeholder="AAPL,NASDAQ&#10;GOOGL,NASDAQ&#10;TSLA,NASDAQ"
            />
          </div>

          <div className="bg-blue-50 p-4 rounded-lg">
            <h3 className="font-medium text-blue-900 mb-2">Example Format:</h3>
            <pre className="text-sm text-blue-800">
{`AAPL,NASDAQ
MSFT,NASDAQ
JPM,NYSE
GS,NYSE`}
            </pre>
          </div>
        </div>

        <div className="flex gap-3">
          <button
            onClick={onClose}
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSubmit}
            disabled={!fileContent && !textInput}
            className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            <Upload size={16} />
            Upload Symbols
          </button>
        </div>
      </div>
    </div>
  );
}