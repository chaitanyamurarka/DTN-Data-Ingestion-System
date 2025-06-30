import { apiClient } from './api-client.js';
import { showToast, formatDateTime } from './utils.js';

export class SymbolManager {
    constructor() {
        this.symbols = [];
        this.currentPage = 0;
        this.pageSize = 50;
        this.totalSymbols = 0;
        this.isLoading = false;
        this.filters = {
            exchanges: [],
            security_types: [],
            search_text: '',
            active: true
        };
    }

    async init() {
        this.setupEventListeners();
        await this.loadSymbols();
    }

    setupEventListeners() {
        const addForm = document.getElementById('add-symbol-form');
        if (addForm) {
            addForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.addSymbol(new FormData(addForm));
            });
        }

        const searchInput = document.getElementById('symbol-search');
        if (searchInput) {
            searchInput.addEventListener('input', this.debounce(() => {
                this.filters.search_text = searchInput.value;
                this.currentPage = 0;
                this.loadSymbols();
            }, 300));
        }

        document.querySelectorAll('.filter-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', () => this.updateFilters());
        });

        // Event listener for pagination buttons
        document.body.addEventListener('click', (e) => {
            if (e.target.matches('.pagination-btn')) {
                e.preventDefault();
                const newPage = parseInt(e.target.dataset.page, 10);
                if (!isNaN(newPage)) {
                    this.currentPage = newPage;
                    this.loadSymbols();
                }
            }
        });

        document.body.addEventListener('click', async (e) => {
            if (e.target.closest('#symbol-lookup-btn')) {
                const input = document.getElementById('symbol-lookup-input');
                const query = input.value.trim();
                if (query) {
                    await this.lookupSymbol(query);
                } else {
                    showToast('Please enter a symbol to search.', 'warning');
                }
            }
        });
    }

    updateFilters() {
        this.currentPage = 0;
        this.loadSymbols();
    }

    async loadSymbols() {
        if (this.isLoading) return;
        
        this.isLoading = true;
        this.showLoadingState();
        
        try {
            const params = {
                exchanges: this.filters.exchanges.length > 0 ? this.filters.exchanges : undefined,
                security_types: this.filters.security_types.length > 0 ? this.filters.security_types : undefined,
                search_text: this.filters.search_text || undefined,
                active: this.filters.active,
                limit: this.pageSize,
                offset: this.currentPage * this.pageSize
            };

            // Remove undefined values
            Object.keys(params).forEach(key => {
                if (params[key] === undefined) {
                    delete params[key];
                }
            });

            console.log('Loading symbols with params:', params);
            const response = await apiClient.get('/symbols/search', params);
            
            console.log('Symbol search response:', response);
            
            // Handle different response formats
            this.symbols = Array.isArray(response) ? response : (response.data || []);
            
            // Estimate total for pagination
            if (this.symbols.length < this.pageSize) {
                this.totalSymbols = this.currentPage * this.pageSize + this.symbols.length;
            } else {
                this.totalSymbols = (this.currentPage + 2) * this.pageSize;
            }

            this.renderSymbolTable();
            
            if (this.symbols.length === 0 && this.currentPage === 0) {
                showToast('No symbols found. Try adjusting your search criteria or add some symbols first.', 'info');
            }

        } catch (error) {
            console.error('Error loading symbols:', error);
            this.symbols = [];
            this.renderSymbolTable();
            showToast('Failed to load symbols. Please check your connection and try again.', 'error');
        } finally {
            this.isLoading = false;
        }
    }

    showLoadingState() {
        const container = document.getElementById('symbols-table-container');
        if (!container) return;

        container.innerHTML = `
            <div class="flex justify-center items-center p-8">
                <span class="loading loading-spinner loading-lg"></span>
                <span class="ml-2">Loading symbols...</span>
            </div>
        `;
    }

    renderSymbolTable() {
        const container = document.getElementById('symbols-table-container');
        if (!container) return;

        if (this.symbols.length === 0) {
            container.innerHTML = `
                <div class="text-center p-8">
                    <div class="mb-4">
                        <i class="fas fa-search text-4xl text-gray-400"></i>
                    </div>
                    <h3 class="text-lg font-semibold mb-2">No Symbols Found</h3>
                    <p class="text-gray-600 mb-4">
                        ${this.currentPage === 0 && !this.filters.search_text ? 
                            'No symbols have been added to the system yet.' : 
                            'No symbols match your current search criteria.'}
                    </p>
                    <div class="space-x-2">
                        <button class="btn btn-primary" onclick="document.getElementById('add-symbol-modal').showModal()">
                            <i class="fas fa-plus"></i> Add Symbol
                        </button>
                        ${this.filters.search_text || this.filters.exchanges.length > 0 || this.filters.security_types.length > 0 ?
                            '<button class="btn btn-outline" onclick="window.symbolManager.clearFilters()">Clear Filters</button>' :
                            ''
                        }
                    </div>
                </div>
            `;
            return;
        }

        const tableBody = this.symbols.map(symbol => this.renderSymbolRow(symbol)).join('');

        container.innerHTML = `
            <div class="overflow-x-auto">
                <table class="table table-zebra">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Exchange</th>
                            <th>Type</th>
                            <th>Description</th>
                            <th>Historical Days</th>
                            <th>Backfill Min</th>
                            <th>Status</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableBody}
                    </tbody>
                </table>
            </div>
            ${this.renderPagination()}
            <div class="mt-4 text-sm text-gray-600">
                Showing ${this.symbols.length} symbols 
                ${this.totalSymbols > this.pageSize ? `(${this.currentPage * this.pageSize + 1}-${this.currentPage * this.pageSize + this.symbols.length} of ~${this.totalSymbols})` : ''}
            </div>
        `;
    }

    renderSymbolRow(symbol) {
        return `
            <tr>
                <td class="font-mono font-bold">${symbol.symbol}</td>
                <td><span class="badge badge-outline">${symbol.exchange}</span></td>
                <td><span class="badge badge-ghost">${symbol.security_type}</span></td>
                <td class="max-w-xs truncate" title="${symbol.description || ''}">${symbol.description || 'N/A'}</td>
                <td>${symbol.historical_days || 30}</td>
                <td>${symbol.backfill_minutes || 120}</td>
                <td>
                    <span class="badge ${symbol.active ? 'badge-success' : 'badge-error'}">
                        ${symbol.active ? 'Active' : 'Inactive'}
                    </span>
                </td>
                <td>
                    <div class="btn-group">
                        <button class="btn btn-xs btn-info" onclick="window.symbolManager.viewDataAvailability('${symbol.symbol}')" title="View Data">
                            <i class="fas fa-chart-line"></i>
                        </button>
                        <button class="btn btn-xs btn-warning" onclick="window.symbolManager.editSymbol('${symbol.symbol}')" title="Edit">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-xs btn-error" onclick="window.symbolManager.deleteSymbol('${symbol.symbol}')" title="Delete">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }

    renderPagination() {
        const totalPages = Math.ceil(this.totalSymbols / this.pageSize);
        if (totalPages <= 1) return '';

        let buttons = '';
        
        // Previous button
        if (this.currentPage > 0) {
            buttons += `<button class="join-item btn pagination-btn" data-page="${this.currentPage - 1}">«</button>`;
        }
        
        // Page numbers
        for (let i = 0; i < totalPages; i++) {
            // Limit the number of pages shown
            if (totalPages > 10) {
                if (i > 2 && i < totalPages - 3 && Math.abs(i - this.currentPage) > 2) {
                    if (i === 3 || i === totalPages - 4) {
                        buttons += `<button class="join-item btn btn-disabled">...</button>`;
                    }
                    continue;
                }
            }

            const isActive = i === this.currentPage ? 'btn-active' : '';
            buttons += `<button class="join-item btn pagination-btn ${isActive}" data-page="${i}">${i + 1}</button>`;
        }
        
        // Next button
        if (this.currentPage < totalPages - 1) {
            buttons += `<button class="join-item btn pagination-btn" data-page="${this.currentPage + 1}">»</button>`;
        }

        return `
            <div class="join mt-4 flex justify-center">
                ${buttons}
            </div>
        `;
    }

    clearFilters() {
        this.filters = {
            exchanges: [],
            security_types: [],
            search_text: '',
            active: true
        };
        this.currentPage = 0;
        
        // Clear UI
        const searchInput = document.getElementById('symbol-search');
        if (searchInput) searchInput.value = '';
        
        document.querySelectorAll('.filter-checkbox').forEach(checkbox => {
            checkbox.checked = false;
        });
        
        this.loadSymbols();
    }

    async lookupSymbol(query) {
        const resultsContainer = document.getElementById('symbol-lookup-results');
        resultsContainer.innerHTML = '<div class="text-center"><span class="loading loading-spinner"></span></div>';

        try {
            const results = await apiClient.get(`/symbols/lookup/${query}`);
            if (results && results.length > 0) {
                this.renderLookupResults(results);
            } else {
                resultsContainer.innerHTML = '<p class="text-center">No symbols found for that query.</p>';
            }
        } catch (error) {
            resultsContainer.innerHTML = `<div class="alert alert-error"><span>${error.message}</span></div>`;
            console.error('Lookup error:', error);
        }
    }

    renderLookupResults(results) {
        const resultsContainer = document.getElementById('symbol-lookup-results');
        const tableRows = results.map(res => `
            <tr>
                <td><strong class="font-mono">${res.symbol}</strong></td>
                <td><span class="badge badge-ghost">${res.exchange}</span></td>
                <td><span class="badge badge-outline">${res.security_type}</span></td>
                <td class="text-xs truncate max-w-xs" title="${res.description || ''}">${res.description || 'N/A'}</td>
                <td>
                    <button class="btn btn-xs btn-primary select-symbol-btn"
                            data-symbol='${JSON.stringify(res)}'>
                        Select
                    </button>
                </td>
            </tr>
        `).join('');

        resultsContainer.innerHTML = `
            <div class="overflow-x-auto">
                <table class="table table-compact w-full">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Exchange</th>
                            <th>Type</th>
                            <th>Description</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${tableRows}
                    </tbody>
                </table>
            </div>
        `;

        // Add event listeners for the new "Select" buttons
        document.querySelectorAll('.select-symbol-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const symbolData = JSON.parse(e.currentTarget.dataset.symbol);
                this.addSelectedSymbol(symbolData);
            });
        });
    }

    async addSelectedSymbol(symbolData) {
        const dataToAdd = {
            symbol: symbolData.symbol,
            exchange: symbolData.exchange,
            security_type: symbolData.security_type,
            description: symbolData.description || '',
            historical_days: 30,
            backfill_minutes: 120,
            added_by: 'admin_lookup',
        };

        try {
            await apiClient.post('/symbols/', dataToAdd);
            showToast(`Symbol ${symbolData.symbol} added successfully!`, 'success');
            document.getElementById('add-symbol-modal').close();
            await this.loadSymbols();
        } catch (error) {
            showToast(error.message || `Failed to add ${symbolData.symbol}`, 'error');
            console.error('Error adding selected symbol:', error);
        }
    }

    async addSymbol(formData) {
        try {
            const data = Object.fromEntries(formData);
            data.historical_days = parseInt(data.historical_days);
            data.backfill_minutes = parseInt(data.backfill_minutes);
            data.added_by = 'admin';

            await apiClient.post('/symbols/', data);
            showToast('Symbol added successfully', 'success');
            document.getElementById('add-symbol-modal').close();
            document.getElementById('add-symbol-form').reset();
            await this.loadSymbols();
        } catch (error) {
            showToast(error.message || 'Failed to add symbol', 'error');
            console.error(error);
        }
    }

    async viewDataAvailability(symbol) {
        showToast(`Data availability view for ${symbol} not implemented yet.`, 'info');
    }

    editSymbol(symbol) {
        showToast(`Edit functionality for ${symbol} not implemented yet.`, 'info');
    }

    async deleteSymbol(symbol) {
        if (!confirm(`Are you sure you want to deactivate ${symbol}?`)) {
            return;
        }
        
        try {
            await apiClient.delete(`/symbols/${symbol}`);
            showToast(`Symbol ${symbol} deactivated successfully`, 'success');
            await this.loadSymbols();
        } catch (error) {
            showToast(`Failed to deactivate ${symbol}`, 'error');
            console.error(error);
        }
    }

    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

// Ensure the manager is globally accessible for inline event handlers
window.symbolManager = new SymbolManager();