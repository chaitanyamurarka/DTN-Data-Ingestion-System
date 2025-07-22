export interface Symbol {
  symbol: string;
  exchange: string;
  description?: string;
  securityType?: string;
}

export interface SymbolUpdate {
  symbol: string;
  exchange: string;
}

export interface SearchParams {
  search_string?: string;
  exchange?: string;
  security_type?: string;
}