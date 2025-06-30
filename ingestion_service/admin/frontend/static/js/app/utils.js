/**
 * Shows a toast notification.
 * @param {string} message - The message to display.
 * @param {string} type - The type of toast ('success', 'error', 'warning', 'info').
 */
export function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `alert alert-${type} shadow-lg`;
    toast.innerHTML = `
        <div>
            <span>${message}</span>
        </div>
    `;

    container.appendChild(toast);

    setTimeout(() => {
        toast.remove();
    }, 5000);
}

/**
 * Formats a date-time string into a more readable format.
 * @param {string} dateTimeString - The ISO date-time string.
 * @returns {string} - The formatted date-time.
 */
export function formatDateTime(dateTimeString) {
    if (!dateTimeString) return 'N/A';
    try {
        const date = new Date(dateTimeString);
        return date.toLocaleString();
    } catch (error) {
        console.error('Error formatting date:', error);
        return 'Invalid Date';
    }
}