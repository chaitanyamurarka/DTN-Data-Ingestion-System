import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { searchSymbols, addSymbol, setSymbols, getIngestedSymbols } from '../lib/api';
import { SearchParams, IngestedSymbol } from '../lib/types';

export const useSearchSymbols = (params: SearchParams, enabled: boolean) => {
  return useQuery({
    queryKey: ['symbols', params],
    queryFn: () => searchSymbols(params),
    enabled,
  });
};

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