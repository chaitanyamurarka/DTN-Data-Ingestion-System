export interface Symbol {
    symbol: string;
    description: string;
    exchange: string;
    securityType: string;
}
  
export interface SearchParams {
    search_string?: string;
    exchange?: string;
    security_type?: string;
}

// New type for ingested symbols
export interface IngestedSymbol {
    symbol: string;
    exchange: string;
}