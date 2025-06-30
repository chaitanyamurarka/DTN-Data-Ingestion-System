import { SymbolManager } from './app/symbol-management.js';
import { ScheduleManager } from './app/schedule-config.js';
import { DataMonitor } from './app/data-monitor.js';
import { SymbolImporter } from './app/symbol-importer.js';

class AdminDashboard {
    constructor() {
        this.currentPage = 'symbols';
        this.managers = {
            symbols: new SymbolManager(),
            schedules: new ScheduleManager(),
            monitor: new DataMonitor(),
            importer: new SymbolImporter()
        };
    }
    
    async init() {
        this.setupNavigation();
        this.setupThemeToggle();
        await this.loadPage(this.currentPage);
    }
    
    setupNavigation() {
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', async (e) => {
                e.preventDefault();
                const page = link.dataset.page;
                await this.loadPage(page);
                
                // Update active state
                document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
                link.classList.add('active');
                
                // Update URL
                window.history.pushState({page}, '', `#${page}`);
            });
        });
        
        // Handle browser back/forward
        window.addEventListener('popstate', (e) => {
            if (e.state?.page) {
                this.loadPage(e.state.page);
            }
        });
    }
    
    setupThemeToggle() {
        const toggle = document.getElementById('theme-toggle');
        const theme = localStorage.getItem('adminTheme') || 'light';
        
        document.documentElement.setAttribute('data-theme', theme);
        toggle.checked = theme === 'dark';
        
        toggle.addEventListener('change', () => {
            const newTheme = toggle.checked ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', newTheme);
            localStorage.setItem('adminTheme', newTheme);
        });
    }
    
    async loadPage(page) {
        const content = document.getElementById('main-content');
        content.innerHTML = '<div class="flex justify-center p-8"><span class="loading loading-spinner loading-lg"></span></div>';

        this.currentPage = page;

        try {
            switch (page) {
                case 'symbols':
                    content.innerHTML = this.getSymbolsPageHTML();
                    await this.managers.symbols.init();
                    break;

                case 'import':
                    content.innerHTML = this.getImportPageHTML();
                    // Use the global symbolImporter instance to initialize
                    await window.symbolImporter.init();
                    break;

                case 'schedules':
                    content.innerHTML = this.getSchedulesPageHTML();
                    await this.managers.schedules.init();
                    break;

                case 'availability':
                    content.innerHTML = this.getAvailabilityPageHTML();
                    await this.managers.monitor.initAvailability();
                    break;

                case 'statistics':
                    content.innerHTML = this.getStatisticsPageHTML();
                    await this.managers.monitor.initStatistics();
                    break;

                default:
                    content.innerHTML = '<div class="alert alert-error">Page not found</div>';
            }
        } catch (error) {
            console.error('Error loading page:', error);
            content.innerHTML = `<div class="alert alert-error">Failed to load page: ${error.message}</div>`;
        }
    }
    
    getSymbolsPageHTML() {
        return `
            <div class="space-y-4">
                <div class="flex justify-between items-center">
                    <h1 class="text-3xl font-bold">Symbol Management</h1>
                    <button class="btn btn-primary" onclick="document.getElementById('add-symbol-modal').showModal()">
                        <i class="fas fa-plus"></i> Add Symbol
                    </button>
                </div>
                
                <div class="card bg-base-100 shadow-xl">
                    <div class="card-body">
                        <div class="flex flex-wrap gap-4 mb-4">
                            <input type="text" id="symbol-search" placeholder="Search symbols..." 
                                   class="input input-bordered flex-1">
                            
                            <details class="dropdown">
                                <summary class="btn btn-outline">
                                    <i class="fas fa-filter"></i> Filters
                                </summary>
                                <div class="dropdown-content z-[1] p-4 shadow bg-base-100 rounded-box w-80">
                                    <div class="space-y-4">
                                        <div>
                                            <h4 class="font-bold mb-2">Exchanges</h4>
                                            <div class="space-y-2">
                                                ${['NYSE', 'NASDAQ', 'CME', 'EUREX'].map(ex => `
                                                    <label class="label cursor-pointer">
                                                        <span class="label-text">${ex}</span>
                                                        <input type="checkbox" class="checkbox filter-checkbox" 
                                                               data-filter="exchange" value="${ex}">
                                                    </label>
                                                `).join('')}
                                            </div>
                                        </div>
                                        <div>
                                            <h4 class="font-bold mb-2">Security Types</h4>
                                            <div class="space-y-2">
                                                ${['STOCK', 'FUTURE', 'OPTION', 'INDEX'].map(type => `
                                                    <label class="label cursor-pointer">
                                                        <span class="label-text">${type}</span>
                                                        <input type="checkbox" class="checkbox filter-checkbox" 
                                                               data-filter="security_type" value="${type}">
                                                    </label>
                                                `).join('')}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </details>
                            
                            <label class="label cursor-pointer">
                                <span class="label-text mr-2">Active Only</span>
                                <input type="checkbox" class="toggle" id="active-only-filter" checked>
                            </label>
                        </div>
                        
                        <div id="symbols-table-container">
                            </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    getSchedulesPageHTML() {
        return `
            <div class="space-y-4">
                <h1 class="text-3xl font-bold">Schedule Configuration</h1>
                <div id="schedules-container">
                    <!-- Schedules will be rendered here -->
                </div>
            </div>
        `;
    }
    
    getImportPageHTML() {
        return `
            <div class="space-y-4">
                <h1 class="text-3xl font-bold">Import Symbols</h1>
                <div class="card bg-base-100 shadow-xl">
                    <div class="card-body">
                        <h2 class="card-title">Import from DTN Symbol Files</h2>
                        <p>Upload the by_exchange.zip file from DTN to import symbols for NYSE, NASDAQ, CME, and EUREX exchanges.</p>
                        
                        <div class="form-control w-full max-w-xs mt-4">
                            <label class="label">
                                <span class="label-text">Select DTN zip file</span>
                            </label>
                            <input type="file" id="dtn-file-input" accept=".zip" 
                                   class="file-input file-input-bordered w-full max-w-xs" />
                        </div>
                        
                        <div class="mt-4">
                            <button class="btn btn-primary" onclick="symbolImporter.importFromFile()">
                                <i class="fas fa-upload"></i> Start Import
                            </button>
                        </div>
                        
                        <div id="import-progress" class="mt-6" style="display: none;">
                            <h3 class="font-bold mb-2">Import Progress</h3>
                            <progress class="progress progress-primary w-full" value="0" max="100"></progress>
                            <div id="import-log" class="mt-4 p-4 bg-base-200 rounded-lg h-64 overflow-y-auto font-mono text-sm">
                                <!-- Import logs will appear here -->
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
    
    getAvailabilityPageHTML() {
        return `
            <div class="space-y-4">
                <h1 class="text-3xl font-bold">Data Availability</h1>
                
                <div class="form-control w-full max-w-xs">
                    <label class="label">
                        <span class="label-text">Select Symbol</span>
                    </label>
                    <select id="availability-symbol-select" class="select select-bordered">
                        <option value="">Choose a symbol...</option>
                    </select>
                </div>
                
                <div id="availability-display" class="hidden">
                    <!-- Availability data will be shown here -->
                </div>
            </div>
        `;
    }
    
    getStatisticsPageHTML() {
        return `
            <div class="space-y-4">
                <h1 class="text-3xl font-bold">Ingestion Statistics</h1>
                
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <div class="stat bg-base-100 shadow rounded-lg">
                        <div class="stat-figure text-primary">
                            <i class="fas fa-chart-line text-3xl"></i>
                        </div>
                        <div class="stat-title">Total Symbols</div>
                        <div class="stat-value" id="stat-total-symbols">-</div>
                    </div>
                    
                    <div class="stat bg-base-100 shadow rounded-lg">
                        <div class="stat-figure text-success">
                            <i class="fas fa-check-circle text-3xl"></i>
                        </div>
                        <div class="stat-title">Active Symbols</div>
                        <div class="stat-value text-success" id="stat-active-symbols">-</div>
                    </div>
                    
                    <div class="stat bg-base-100 shadow rounded-lg">
                        <div class="stat-figure text-info">
                            <i class="fas fa-database text-3xl"></i>
                        </div>
                        <div class="stat-title">Data Points</div>
                        <div class="stat-value text-info" id="stat-data-points">-</div>
                    </div>
                    
                    <div class="stat bg-base-100 shadow rounded-lg">
                        <div class="stat-figure text-warning">
                            <i class="fas fa-hdd text-3xl"></i>
                        </div>
                        <div class="stat-title">Disk Usage</div>
                        <div class="stat-value text-warning" id="stat-disk-usage">-</div>
                    </div>
                </div>
                
                <div class="card bg-base-100 shadow-xl">
                    <div class="card-body">
                        <h2 class="card-title">Recent Ingestion Activity</h2>
                        <div id="activity-timeline">
                            <!-- Activity timeline will be rendered here -->
                        </div>
                    </div>
                </div>
            </div>
        `;
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', async () => {
    const dashboard = new AdminDashboard();
    await dashboard.init();
});