import { apiClient } from './api-client.js';
import { showToast } from './utils.js';

export class ScheduleManager {
    constructor() {
        this.schedules = [];
        this.symbols = [];
    }
    
    async init() {
        await this.loadSymbols();
        await this.loadSchedules();
        this.setupEventListeners();
    }
    
    async loadSymbols() {
        try {
            const response = await apiClient.get('/symbols/search', {
                params: { active: true, limit: 1000 }
            });
            this.symbols = response.data;
        } catch (error) {
            console.error('Failed to load symbols:', error);
        }
    }
    
    async loadSchedules() {
        try {
            const response = await apiClient.get('/schedules/');
            this.schedules = response.data;
            this.renderSchedules();
        } catch (error) {
            showToast('Failed to load schedules', 'error');
        }
    }
    
    renderSchedules() {
        const container = document.getElementById('schedules-container');
        if (!container) return;
        
        const groupedSchedules = this.groupSchedulesBySymbol();
        
        container.innerHTML = `
            <div class="space-y-6">
                ${Object.entries(groupedSchedules).map(([symbol, schedules]) => `
                    <div class="card bg-base-100 shadow-xl">
                        <div class="card-body">
                            <h2 class="card-title">
                                <i class="fas fa-chart-line"></i>
                                ${symbol}
                            </h2>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
                                ${this.renderScheduleCard(symbol, 'historical', schedules.historical)}
                                ${this.renderScheduleCard(symbol, 'live', schedules.live)}
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
            
            <button class="btn btn-primary fixed bottom-4 right-4" onclick="scheduleManager.showAddScheduleModal()">
                <i class="fas fa-plus"></i> Add Schedule
            </button>
        `;
    }
    
    renderScheduleCard(symbol, type, schedule) {
        const isHistorical = type === 'historical';
        const icon = isHistorical ? 'fa-history' : 'fa-broadcast-tower';
        const title = isHistorical ? 'Historical Data' : 'Live Data';
        
        if (!schedule) {
            return `
                <div class="stat bg-base-200 rounded-lg">
                    <div class="stat-figure text-secondary">
                        <i class="fas ${icon} text-3xl"></i>
                    </div>
                    <div class="stat-title">${title}</div>
                    <div class="stat-value text-base-300">Not Configured</div>
                    <div class="stat-actions mt-2">
                        <button class="btn btn-sm btn-primary" 
                                onclick="scheduleManager.configureSchedule('${symbol}', '${type}')">
                            Configure
                        </button>
                    </div>
                </div>
            `;
        }
        
        const nextRun = schedule.next_run ? new Date(schedule.next_run).toLocaleString() : 'Not scheduled';
        const statusClass = schedule.enabled ? 'badge-success' : 'badge-error';
        
        return `
            <div class="stat bg-base-200 rounded-lg">
                <div class="stat-figure text-secondary">
                    <i class="fas ${icon} text-3xl"></i>
                </div>
                <div class="stat-title">${title}</div>
                <div class="stat-value">
                    <span class="badge ${statusClass}">
                        ${schedule.enabled ? 'Enabled' : 'Disabled'}
                    </span>
                </div>
                <div class="stat-desc">
                    <div>Schedule: ${this.formatCronExpression(schedule.cron_expression)}</div>
                    <div>Next run: ${nextRun}</div>
                </div>
                <div class="stat-actions mt-2">
                    <button class="btn btn-sm btn-warning" 
                            onclick="scheduleManager.editSchedule('${schedule.id}')">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-sm ${schedule.enabled ? 'btn-error' : 'btn-success'}" 
                            onclick="scheduleManager.toggleSchedule('${schedule.id}')">
                        <i class="fas ${schedule.enabled ? 'fa-pause' : 'fa-play'}"></i>
                    </button>
                </div>
            </div>
        `;
    }
    
    groupSchedulesBySymbol() {
        const grouped = {};
        this.schedules.forEach(schedule => {
            if (!grouped[schedule.symbol]) {
                grouped[schedule.symbol] = {};
            }
            grouped[schedule.symbol][schedule.schedule_type] = schedule;
        });
        return grouped;
    }
    
    formatCronExpression(cron) {
        // Simple cron to human readable format
        const parts = cron.split(' ');
        if (parts[1] === '*' && parts[0] === '0') {
            return 'Every hour';
        } else if (parts[1] !== '*' && parts[0] !== '*') {
            return `Daily at ${parts[1]}:${parts[0].padStart(2, '0')}`;
        }
        return cron;
    }
    
    async configureSchedule(symbol, type) {
        // Show configuration modal
        const modal = this.createScheduleModal(symbol, type);
        document.body.appendChild(modal);
        modal.showModal();
    }
    
    createScheduleModal(symbol, type, existing = null) {
        const modal = document.createElement('dialog');
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-box">
                <h3 class="font-bold text-lg">
                    ${existing ? 'Edit' : 'Configure'} ${type} Schedule - ${symbol}
                </h3>
                <form id="schedule-form" class="space-y-4 mt-4">
                    <input type="hidden" name="symbol" value="${symbol}">
                    <input type="hidden" name="schedule_type" value="${type}">
                    
                    <div class="form-control">
                        <label class="label">
                            <span class="label-text">Schedule Type</span>
                        </label>
                        <select name="schedule_preset" class="select select-bordered" 
                                onchange="scheduleManager.updateCronPreview(this.value)">
                            <option value="0 20 * * 1-5">Daily at 8 PM ET (Weekdays)</option>
                            <option value="0 */1 * * *">Every Hour</option>
                            <option value="0 */6 * * *">Every 6 Hours</option>
                            <option value="0 0 * * 1-5">Daily at Midnight (Weekdays)</option>
                            <option value="custom">Custom Cron Expression</option>
                        </select>
                    </div>
                    
                    <div class="form-control" id="custom-cron-input" style="display: none;">
                        <label class="label">
                            <span class="label-text">Cron Expression</span>
                        </label>
                        <input type="text" name="cron_expression" class="input input-bordered" 
                               placeholder="0 20 * * 1-5" value="${existing?.cron_expression || '0 20 * * 1-5'}">
                    </div>
                    
                    ${type === 'historical' ? `
                        <div class="form-control">
                            <label class="label">
                                <span class="label-text">Intervals to Download</span>
                            </label>
                            <div class="grid grid-cols-3 gap-2">
                                ${['1s', '5s', '10s', '15s', '30s', '45s', '1m', '5m', '10m', '15m', '30m', '45m', '1h', '1d']
                                    .map(interval => `
                                        <label class="label cursor-pointer">
                                            <span class="label-text">${interval}</span>
                                            <input type="checkbox" name="intervals" value="${interval}" 
                                                   class="checkbox checkbox-sm" checked>
                                        </label>
                                    `).join('')}
                            </div>
                        </div>
                    ` : `
                        <div class="form-control">
                            <label class="label">
                                <span class="label-text">Live Data Settings</span>
                            </label>
                            <label class="label cursor-pointer">
                                <span class="label-text">Auto-start on schedule</span>
                                <input type="checkbox" name="auto_start" class="toggle" checked>
                            </label>
                            <label class="label cursor-pointer">
                                <span class="label-text">Auto-stop after market close</span>
                                <input type="checkbox" name="auto_stop" class="toggle" checked>
                            </label>
                        </div>
                    `}
                    
                    <div class="form-control">
                        <label class="label cursor-pointer">
                            <span class="label-text">Enable Schedule</span>
                            <input type="checkbox" name="enabled" class="toggle toggle-primary" 
                                   ${existing?.enabled !== false ? 'checked' : ''}>
                        </label>
                    </div>
                    
                    <div class="modal-action">
                        <button type="submit" class="btn btn-primary">Save Schedule</button>
                        <button type="button" class="btn" onclick="this.closest('dialog').close()">Cancel</button>
                    </div>
                </form>
            </div>
            <form method="dialog" class="modal-backdrop">
                <button>close</button>
            </form>
        `;
        
        // Add form submit handler
        modal.querySelector('#schedule-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            await this.saveSchedule(new FormData(e.target));
            modal.close();
        });
        
        return modal;
    }
    
    async saveSchedule(formData) {
        try {
            const data = {
                symbol: formData.get('symbol'),
                schedule_type: formData.get('schedule_type'),
                cron_expression: formData.get('cron_expression'),
                enabled: formData.get('enabled') === 'on',
                config: {}
            };
            
            // Add type-specific config
            if (data.schedule_type === 'historical') {
                data.config.intervals = formData.getAll('intervals');
            } else {
                data.config.auto_start = formData.get('auto_start') === 'on';
                data.config.auto_stop = formData.get('auto_stop') === 'on';
            }
            
            await apiClient.post('/schedules/', data);
            showToast('Schedule saved successfully', 'success');
            await this.loadSchedules();
        } catch (error) {
            showToast('Failed to save schedule', 'error');
            console.error(error);
        }
    }
}

window.scheduleManager = new ScheduleManager();