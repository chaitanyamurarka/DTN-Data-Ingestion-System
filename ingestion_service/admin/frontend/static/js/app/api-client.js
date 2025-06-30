import { showToast } from './utils.js';

class ApiClient {
    constructor(baseURL = '/api') {
        this.baseURL = baseURL;
    }

    async _fetch(url, options = {}) {
        // Do not set Content-Type for FormData; the browser does it automatically
        const isFormData = options.body instanceof FormData;
        const headers = isFormData ? {} : { 'Content-Type': 'application/json' };

        try {
            const response = await fetch(`${this.baseURL}${url}`, {
                ...options,
                headers: { ...headers, ...options.headers },
            });

            if (!response.ok) {
                // Try to parse error JSON, but fall back to a generic message
                const errorData = await response.json().catch(() => ({
                    detail: `Request failed with status: ${response.status}`
                }));
                throw new Error(errorData.detail);
            }

            if (response.status === 204) { // No Content
                return null;
            }

            return await response.json();
        } catch (error) {
            console.error('API Client Error:', error);
            showToast(error.message, 'error');
            throw error;
        }
    }

    get(url, params = {}) {
        const query = new URLSearchParams(params).toString();
        return this._fetch(query ? `${url}?${query}` : url);
    }

    post(url, data) {
        const isFormData = data instanceof FormData;
        return this._fetch(url, {
            method: 'POST',
            body: isFormData ? data : JSON.stringify(data),
        });
    }

    patch(url, data) {
        return this._fetch(url, {
            method: 'PATCH',
            body: JSON.stringify(data),
        });
    }

    delete(url) {
        return this._fetch(url, {
            method: 'DELETE',
        });
    }
}

export const apiClient = new ApiClient();