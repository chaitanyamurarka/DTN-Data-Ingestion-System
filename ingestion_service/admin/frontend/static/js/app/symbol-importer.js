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
        // The event listener for the button click is already in index.html as an onclick attribute.
    }

    async importFromFile() {
        if (!this.fileInput.files.length) {
            showToast('Please select a file to import', 'warning');
            return;
        }

        const file = this.fileInput.files[0];
        // Note: In a real-world scenario, you would upload the file to the server.
        // For this example, we'll simulate the process by passing the file path.
        // This will likely require adjustments in your backend to handle file uploads.
        const filePath = `C:\\fakepath\\${file.name}`; // This is a placeholder

        this.importProgress.style.display = 'block';
        this.importLog.innerHTML = `Starting import of ${file.name}...\n`;

        try {
            // This is a simulated call. The backend endpoint expects a file path.
            // A more robust solution would involve a file upload mechanism.
            const response = await apiClient.post(`/symbols/import/dtn?file_path=${encodeURIComponent(filePath)}`);
            const results = response.data;

            this.importLog.innerHTML += 'Import successful!\n';
            this.importLog.innerHTML += JSON.stringify(results.imported, null, 2);
            showToast('Symbol import completed', 'success');

        } catch (error) {
            this.importLog.innerHTML += `\nError during import: ${error.response?.data?.detail || error.message}\n`;
            showToast('Symbol import failed', 'error');
            console.error('Import error:', error);
        }
    }
}