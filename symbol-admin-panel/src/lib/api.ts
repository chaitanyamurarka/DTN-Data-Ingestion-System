import axios from 'axios';
import { SearchParams, Symbol, IngestedSymbol } from './types';

const API_BASE_URL = 'http://localhost:8500';

const api = axios.create({
  baseURL: API_BASE_URL,
});

export const searchSymbols = async (params: SearchParams): Promise<Symbol[]> => {
  const response = await api.get('/search_symbols/', { params });
  return response.data;
};

// New function to get ingested symbols
export const getIngestedSymbols = async (): Promise<IngestedSymbol[]> => {
  const response = await api.get('/get_ingestion_symbols/');
  return response.data;
};

export const addSymbol = async (symbol: IngestedSymbol): Promise<any> => {
  const response = await api.post('/add_ingestion_symbol/', symbol);
  return response.data;
};

export const setSymbols = async (symbols: IngestedSymbol[]): Promise<any> => {
    const response = await api.post('/set_ingestion_symbols/', symbols);
    return response.data;
};