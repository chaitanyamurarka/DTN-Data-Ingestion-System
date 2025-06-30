import { apiClient } from './api-client.js';
import { showToast, formatDateTime } from './utils.js';

export class SymbolManager {
    constructor() {
        this.symbols = [];
        this.currentPage = 0;
        this.pageSize = 50;
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
        // Add symbol form
        const addForm = document.getElementById('add-symbol-form');
        if (addForm) {
            addForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.addSymbol(new FormData(addForm));
            });
        }
        
        // Search input
        const searchInput = document.getElementById('symbol-search');
        if (searchInput) {
            searchInput.addEventListener('input', this.debounce(() => {
                this.filters.search_text = searchInput.value;
                this.loadSymbols();
            }, 300));
        }
        
        // Filter checkboxes
        document.querySelectorAll('.filter-checkbox').forEach(checkbox => {
            checkbox.addEventListener('change', () => this.updateFilters());
        });
    }
    
    async loadSymbols() {
        try {
            const params = {
                ...this.filters,
                limit: this.pageSize,
                offset: this.currentPage * this.pageSize
            };
            
            const response = await apiClient.get('/symbols/search', { params });
            this.symbols = response.data;
            this.renderSymbolTable();
        } catch (error) {
            showToast('Failed to load symbols', 'error');
            console.error(error);
        }
    }
    
    renderSymbolTable() {
        const container = document.getElementById('symbols-table-container');
        if (!container) return;
        
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
                        ${this.symbols.map(symbol => this.renderSymbolRow(symbol)).join('')}
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
                <td class="max-w-xs truncate">${symbol.description}</td>
                <td>${symbol.historical_days}</td>
                <td>${symbol.backfill_minutes}</td>
                <td>
                    <span class="badge ${symbol.active ? 'badge-success' : 'badge-error'}">
                        ${symbol.active ? 'Active' : 'Inactive'}
                    </span>
                </td>
                <td>
                    <div class="btn-group">
                        <button class="btn btn-xs btn-info" onclick="symbolManager.viewDataAvailability('${symbol.symbol}')">
                            <i class="fas fa-chart-line"></i>
                        </button>
                        <button class="btn btn-xs btn-warning" onclick="symbolManager.editSymbol('${symbol.symbol}')">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-xs btn-error" onclick="symbolManager.deleteSymbol('${symbol.symbol}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            </tr>
        `;
    }
    
    async addSymbol(formData) {
        try {
            const data = Object.fromEntries(formData);
            data.historical_days = parseInt(data.historical_days);
            data.backfill_minutes = parseInt(data.backfill_minutes);
            data.added_by = 'admin'; // Get from session
            
            await apiClient.post('/symbols/', data);
            showToast('Symbol added successfully', 'success');
            document.getElementById('add-symbol-modal').close();
            document.getElementById('add-symbol-form').reset();
            await this.loadSymbols();
        } catch (error) {
            showToast('Failed to add symbol', 'error');
            console.error(error);
        }
    }
    
    async viewDataAvailability(symbol) {
        const modal = document.createElement('dialog');
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-box max-w-4xl">
                <h3 class="font-bold text-lg mb-4">Data Availability: ${symbol}</h3>
                <div id="availability-content" class="space-y-2">
                    <div class="flex justify-center">
                        <span class="loading loading-spinner loading-lg"></span>
                    </div>
                </div>
                <div class="modal-action">
                    <button class="btn" onclick="this.closest('dialog').close()">Close</button>
                </div>
            </div>
            <form method="dialog" class="modal-backdrop">
                <button>close</button>
            </form>
        `;
        
        document.body.appendChild(modal);
        modal.showModal();
        
        try {
            const response = await apiClient.get(`/monitor/availability/${symbol}`);
            const availability = response.data;
            
            const content = document.getElementById('availability-content');
            content.innerHTML = `
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    ${Object.entries(availability).map(([timeframe, data]) => `
                        <div class="stat bg-base-100 rounded-lg shadow">
                            <div class="stat-title">${timeframe}</div>
                            ${data.error ? `
                                <div class="stat-value text-error">Error</div>
                                <div class="stat-desc">${data.error}</div>
                            ` : data.first_timestamp ? `
                                <div class="stat-value text-primary">${data.data_points.toLocaleString()}</div>
                                <div class="stat-desc">
                                    ${formatDateTime(data.first_timestamp)} - ${formatDateTime(data.last_timestamp)}
                                    <br>${data.duration_days} days
                                </div>
                            ` : `
                                <div class="stat-value text-base-300">No Data</div>
                                <div class="stat-desc">No data available</div>
                            `}
                        </div>
                    `).join('')}
                </div>
            `;
        } catch (error) {
            console.error(error);
            document.getElementById('availability-content').innerHTML = `
                <div class="alert alert-error">
                    <i class="fas fa-exclamation-circle"></i>
                    <span>Failed to load data availability</span>
                </div>
            `;
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

// Global instance
window.symbolManager = new SymbolManager();