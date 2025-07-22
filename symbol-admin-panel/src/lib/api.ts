import axios from 'axios';
import { Symbol, SymbolUpdate, SearchParams } from './types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8500';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const symbolApi = {
  searchSymbols: async (params: SearchParams): Promise<Symbol[]> => {
    const response = await api.get('/search_symbols/', { params });
    return response.data;
  },

  setIngestionSymbols: async (symbols: SymbolUpdate[]): Promise<{ message: string }> => {
    const response = await api.post('/set_ingestion_symbols/', symbols);
    return response.data;
  },

  addIngestionSymbol: async (symbol: SymbolUpdate): Promise<{ message: string }> => {
    const response = await api.post('/add_ingestion_symbol/', symbol);
    return response.data;
  },
};