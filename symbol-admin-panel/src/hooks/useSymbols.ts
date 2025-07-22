import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { symbolApi } from '@/lib/api';
import { SearchParams, SymbolUpdate } from '@/lib/types';
import toast from 'react-hot-toast';

export const useSearchSymbols = (params: SearchParams) => {
  return useQuery({
    queryKey: ['symbols', params],
    queryFn: () => symbolApi.searchSymbols(params),
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
  });
};

export const useAddSymbol = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: symbolApi.addIngestionSymbol,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['symbols'] });
      if (data.status === 'skipped') {
        toast.success('Symbol already exists in ingestion list');
      } else {
        toast.success('Symbol added to ingestion list successfully');
      }
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to add symbol');
    },
  });
};

export const useSetSymbols = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: symbolApi.setIngestionSymbols,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['symbols'] });
      toast.success('Ingestion symbols updated successfully');
    },
    onError: (error: any) => {
      toast.error(error.response?.data?.detail || 'Failed to update symbols');
    },
  });
};