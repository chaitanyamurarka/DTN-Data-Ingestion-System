import { apiClient } from './api-client.js';
import { showToast, formatDateTime } from './utils.js';

export class DataMonitor {
    constructor() {
        this.symbols = [];
    }

    async initAvailability() {
        await this.loadSymbols();
        this.setupEventListeners();
    }

    async initStatistics() {
        await this.loadStatistics();
    }

    async loadSymbols() {
        try {
            const response = await apiClient.get('/symbols/search', {
                params: { active: true, limit: 1000 }
            });
            this.symbols = response.data;
            this.populateSymbolSelect();
        } catch (error) {
            console.error('Failed to load symbols for availability check:', error);
            showToast('Failed to load symbols', 'error');
        }
    }

    populateSymbolSelect() {
        const select = document.getElementById('availability-symbol-select');
        if (!select) return;

        this.symbols.forEach(symbol => {
            const option = document.createElement('option');
            option.value = symbol.symbol;
            option.textContent = symbol.symbol;
            select.appendChild(option);
        });
    }

    setupEventListeners() {
        const select = document.getElementById('availability-symbol-select');
        if (select) {
            select.addEventListener('change', () => {
                const symbol = select.value;
                if (symbol) {
                    this.loadAvailabilityForSymbol(symbol);
                } else {
                    document.getElementById('availability-display').classList.add('hidden');
                }
            });
        }
    }

    async loadAvailabilityForSymbol(symbol) {
        const display = document.getElementById('availability-display');
        if (!display) return;

        display.classList.remove('hidden');
        display.innerHTML = '<div class="flex justify-center p-8"><span class="loading loading-spinner loading-lg"></span></div>';

        try {
            const response = await apiClient.get(`/monitor/availability/${symbol}`);
            const availability = response.data;
            this.renderAvailability(availability, symbol);
        } catch (error) {
            console.error(`Failed to load availability for ${symbol}:`, error);
            showToast(`Failed to load availability for ${symbol}`, 'error');
            display.innerHTML = `<div class="alert alert-error">Could not load data for ${symbol}.</div>`;
        }
    }

    renderAvailability(availability, symbol) {
        const display = document.getElementById('availability-display');
        if (!display) return;

        display.innerHTML = `
            <h2 class="text-2xl font-bold mb-4">Data Availability: ${symbol}</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                ${Object.entries(availability).map(([timeframe, data]) => `
                    <div class="card bg-base-100 shadow-xl">
                        <div class="card-body">
                            <h3 class="card-title">${timeframe}</h3>
                            ${data.error ? `
                                <p class="text-error">Error: ${data.error}</p>
                            ` : data.first_timestamp ? `
                                <p><strong>Data Points:</strong> ${data.data_points.toLocaleString()}</p>
                                <p><strong>From:</strong> ${formatDateTime(data.first_timestamp)}</p>
                                <p><strong>To:</strong> ${formatDateTime(data.last_timestamp)}</p>
                                <p><strong>Duration:</strong> ${data.duration_days} days</p>
                            ` : `
                                <p>No data available</p>
                            `}
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    async loadStatistics() {
        try {
            const response = await apiClient.get('/monitor/statistics');
            const stats = response.data;

            document.getElementById('stat-total-symbols').textContent = stats.total_symbols.toLocaleString();
            document.getElementById('stat-active-symbols').textContent = stats.active_symbols.toLocaleString();
            document.getElementById('stat-data-points').textContent = stats.total_data_points.toLocaleString();
            document.getElementById('stat-disk-usage').textContent = `${stats.disk_usage_mb.toFixed(2)} MB`;
        } catch (error) {
            console.error('Failed to load ingestion statistics:', error);
            showToast('Failed to load statistics', 'error');
        }
    }
}