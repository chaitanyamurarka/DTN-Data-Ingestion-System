import { apiClient } from './api-client.js';
import { showToast, formatDateTime } from './utils.js';

export class SymbolManager {
    constructor() {
        this.symbols = [];
        this.currentPage = 0;
        this.pageSize = 50;
        this.totalSymbols = 0; // Keep track of the total number of symbols
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
                this.currentPage = 0; // Reset to first page on new search
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
        // Implementation for filter updates
        this.currentPage = 0;
        this.loadSymbols();
    }

    async loadSymbols() {
        try {
            const params = {
                exchanges: this.filters.exchanges,
                security_types: this.filters.security_types,
                search_text: this.filters.search_text,
                active: this.filters.active,
                limit: this.pageSize,
                offset: this.currentPage * this.pageSize
            };

            const response = await apiClient.get('/symbols/search', params);
            this.symbols = response; // The response is the array
            // Assuming the total count might be passed in headers or another endpoint
            // For now, we'll estimate based on the results
            if (response.length < this.pageSize) {
                this.totalSymbols = this.currentPage * this.pageSize + response.length;
            } else {
                // This is an optimistic guess, a proper count from the backend is better
                this.totalSymbols = (this.currentPage + 2) * this.pageSize;
            }

            this.renderSymbolTable();
        } catch (error) {
            this.symbols = [];
            this.renderSymbolTable();
            showToast('Failed to load symbols', 'error');
            console.error(error);
        }
    }

    renderSymbolTable() {
        const container = document.getElementById('symbols-table-container');
        if (!container) return;

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
                        ${tableBody.length > 0 ? tableBody : '<tr><td colspan="8" class="text-center">No symbols found.</td></tr>'}
                    </tbody>
                </table>
            </div>
            ${this.renderPagination()}
        `;
    }

    renderSymbolRow(symbol) {
        return `
            <tr>
                <td class="font-mono font-bold">${symbol.symbol}</td>
                <td><span class="badge badge-outline">${symbol.exchange}</span></td>
                <td><span class="badge badge-ghost">${symbol.security_type}</span></td>
                <td class="max-w-xs truncate">${symbol.description || ''}</td>
                <td>${symbol.historical_days}</td>
                <td>${symbol.backfill_minutes}</td>
                <td>
                    <span class="badge ${symbol.active ? 'badge-success' : 'badge-error'}">
                        ${symbol.active ? 'Active' : 'Inactive'}
                    </span>
                </td>
                <td>
                    <div class="btn-group">
                        <button class="btn btn-xs btn-info" onclick="window.symbolManager.viewDataAvailability('${symbol.symbol}')">
                            <i class="fas fa-chart-line"></i>
                        </button>
                        <button class="btn btn-xs btn-warning" onclick="window.symbolManager.editSymbol('${symbol.symbol}')">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-xs btn-error" onclick="window.symbolManager.deleteSymbol('${symbol.symbol}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }

    // **** ADD THIS NEW FUNCTION ****
    renderPagination() {
        const totalPages = Math.ceil(this.totalSymbols / this.pageSize);
        if (totalPages <= 1) return '';

        let buttons = '';
        for (let i = 0; i < totalPages; i++) {
             // Limit the number of pages shown for brevity
            if (totalPages > 10 && (i > 2 && i < totalPages - 3)) {
                if (i === 3) buttons += `<button class="join-item btn btn-disabled">...</button>`;
                continue;
            }

            const isActive = i === this.currentPage ? 'btn-active' : '';
            buttons += `<button class="join-item btn pagination-btn ${isActive}" data-page="${i}">${i + 1}</button>`;
        }

        return `
            <div class="join mt-4 flex justify-center">
                ${buttons}
            </div>
        `;
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
                <td class="text-xs truncate max-w-xs">${res.description || 'N/A'}</td>
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
        // This is similar to your old `addSymbol` method, but uses the data from the lookup
        const dataToAdd = {
            symbol: symbolData.symbol,
            exchange: symbolData.exchange,
            security_type: symbolData.security_type,
            description: symbolData.description,
            historical_days: 30, // Or prompt the user for these
            backfill_minutes: 120, // Or prompt the user
            added_by: 'admin_lookup',
        };

        try {
            await apiClient.post('/symbols/', dataToAdd);
            showToast(`Symbol ${symbolData.symbol} added successfully!`, 'success');
            document.getElementById('add-symbol-modal').close();
            await this.loadSymbols(); // Refresh the main table
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
        // Implementation remains the same
    }

    // Dummy functions for actions until implemented
    editSymbol(symbol) {
        showToast(`Edit action for ${symbol} not implemented.`, 'info');
    }

    deleteSymbol(symbol) {
        showToast(`Delete action for ${symbol} not implemented.`, 'info');
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