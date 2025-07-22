import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { searchSymbols, addSymbol, setSymbols, getIngestedSymbols } from '../lib/api';
import { SearchParams, IngestedSymbol } from '../lib/types';

export const useSearchSymbols = (params: SearchParams) => {
  return useQuery({
    queryKey: ['symbols', params],
    queryFn: () => searchSymbols(params),
    enabled: !!params.search_string, // Only run query if search_string is present
  });
};

// New hook to fetch ingested symbols
export const useIngestedSymbols = () => {
    return useQuery({
      queryKey: ['ingestedSymbols'],
      queryFn: getIngestedSymbols,
    });
};

export const useAddSymbol = () => {
    const queryClient = useQueryClient();
    return useMutation({
      mutationFn: addSymbol,
      onSuccess: () => {
        // When a symbol is added, refetch the list of ingested symbols
        queryClient.invalidateQueries({ queryKey: ['ingestedSymbols'] });
      },
    });
};
  
export const useSetSymbols = () => {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: (symbols: IngestedSymbol[]) => setSymbols(symbols),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['ingestedSymbols'] });
        },
    });
};