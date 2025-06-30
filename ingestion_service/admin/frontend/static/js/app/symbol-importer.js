import { apiClient } from './api-client.js';
import { showToast } from './utils.js';

export class SymbolImporter {
    constructor() {
        this.fileInput = null;
        this.importProgress = null;
        this.importLog = null;
    }

    init() {
        this.fileInput = document.getElementById('dtn-file-input');
        this.importProgress = document.getElementById('import-progress');
        this.importLog = document.getElementById('import-log');
    }

    async importFromFile() {
        if (!this.fileInput || !this.fileInput.files.length) {
            showToast('Please select a file to import', 'warning');
            return;
        }

        const file = this.fileInput.files[0];
        // Create a FormData object to hold the file data
        const formData = new FormData();
        // The key "file" must match the parameter name in the backend router
        formData.append("file", file);

        this.importProgress.style.display = 'block';
        this.importLog.innerHTML = `Uploading and processing ${file.name}...\n`;

        try {
            // Send the FormData object directly
            const results = await apiClient.post('/symbols/import/dtn', formData);

            this.importLog.innerHTML += 'Import successful!\n';
            this.importLog.innerHTML += JSON.stringify(results.imported, null, 2);
            showToast('Symbol import completed', 'success');

        } catch (error) {
            this.importLog.innerHTML += `\nError during import: ${error.message}\n`;
            showToast('Symbol import failed', 'error');
            console.error('Import error:', error);
        }
    }
}

// Create a global instance for the onclick handler in index.html
window.symbolImporter = new SymbolImporter();