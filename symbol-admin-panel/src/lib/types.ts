export interface Symbol {
    symbol: string;
    description: string;
    exchange: string;
    securityType: string;
}

export interface IngestedSymbol {
    symbol: string;
    exchange: string;
}

export interface SymbolUpdate {
    symbol: string;
    exchange: string;
}