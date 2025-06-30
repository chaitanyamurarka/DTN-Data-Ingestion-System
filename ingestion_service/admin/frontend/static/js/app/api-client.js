import { showToast } from './utils.js';

class ApiClient {
    constructor(baseURL = '/api') {
        this.baseURL = baseURL;
    }

    async _fetch(url, options = {}) {
        try {
            const response = await fetch(`${this.baseURL}${url}`, {
                ...options,
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers,
                },
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
            }

            // Handle cases with no content
            if (response.status === 204) {
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
        return this._fetch(url, {
            method: 'POST',
            body: JSON.stringify(data),
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