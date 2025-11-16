/**
 * Validation Definitions Loader (JavaScript)
 *
 * Provides dynamic loading of validation definitions from the single
 * source of truth (validation_definitions.json) for DataK9 Studio.
 *
 * This eliminates hardcoded validation libraries and ensures perfect
 * sync between the Python framework and JavaScript Studio UI.
 *
 * Author: Daniel Edge
 * Date: November 15, 2025
 */

class ValidationDefinitionLoader {
    constructor() {
        this.definitions = null;
        this.metadata = null;
        this.loaded = false;
    }

    /**
     * Load validation definitions from JSON file
     * @param {string} jsonPath - Path to validation_definitions.json
     * @returns {Promise<void>}
     */
    async load(jsonPath = '../validation_definitions.json') {
        try {
            const response = await fetch(jsonPath);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();

            // Extract metadata
            this.metadata = data._metadata || {};

            // Extract validation definitions
            this.definitions = {};
            for (const [key, value] of Object.entries(data)) {
                if (key !== '$schema' && key !== '_metadata') {
                    this.definitions[key] = value;
                }
            }

            this.loaded = true;
            console.log(`✓ Loaded ${this.getValidationCount()} validation definitions`);
            console.log(`  Version: ${this.metadata.version || 'unknown'}`);
            console.log(`  Last Updated: ${this.metadata.last_updated || 'unknown'}`);

        } catch (error) {
            console.error('Failed to load validation definitions:', error);
            throw error;
        }
    }

    /**
     * Get all validation definitions
     * @returns {Object} All validation definitions
     */
    getAllDefinitions() {
        this._ensureLoaded();
        return {...this.definitions};
    }

    /**
     * Get definition for a specific validation type
     * @param {string} validationType - Validation type name
     * @returns {Object|null} Validation definition or null if not found
     */
    getDefinition(validationType) {
        this._ensureLoaded();
        return this.definitions[validationType] || null;
    }

    /**
     * Get all validations in a specific category
     * @param {string} category - Category name
     * @returns {Object} Validations in that category
     */
    getByCategory(category) {
        this._ensureLoaded();
        const result = {};
        for (const [name, defn] of Object.entries(this.definitions)) {
            if (defn.category === category) {
                result[name] = defn;
            }
        }
        return result;
    }

    /**
     * Get list of all unique categories
     * @returns {Array<string>} Sorted category names
     */
    getCategories() {
        this._ensureLoaded();
        const categories = new Set();
        for (const defn of Object.values(this.definitions)) {
            if (defn.category) {
                categories.add(defn.category);
            }
        }
        return Array.from(categories).sort();
    }

    /**
     * Get sorted list of all validation type names
     * @returns {Array<string>} Sorted validation type names
     */
    listValidationTypes() {
        this._ensureLoaded();
        return Object.keys(this.definitions).sort();
    }

    /**
     * Get parameter definitions for a validation type
     * @param {string} validationType - Validation type name
     * @returns {Array} Parameter definitions
     */
    getParamDefinitions(validationType) {
        this._ensureLoaded();
        const defn = this.getDefinition(validationType);
        return defn ? (defn.params || []) : [];
    }

    /**
     * Get total number of validation definitions
     * @returns {number} Count of validations
     */
    getValidationCount() {
        this._ensureLoaded();
        return Object.keys(this.definitions).length;
    }

    /**
     * Get metadata about the definitions file
     * @returns {Object} Metadata
     */
    getMetadata() {
        this._ensureLoaded();
        return {...this.metadata};
    }

    /**
     * Convert to format compatible with legacy validationLibrary
     * @returns {Object} Validation library object
     */
    toValidationLibrary() {
        this._ensureLoaded();
        const library = {};

        for (const [name, defn] of Object.entries(this.definitions)) {
            library[name] = {
                icon: defn.icon || '✓',
                name: name.replace('Check', ' Check').replace('Validation', ' Validation'),
                type: name,
                category: defn.category || 'Other',
                description: defn.description || '',
                params: defn.params || [],
                examples: defn.examples || '',
                tips: defn.tips || ''
            };
        }

        return library;
    }

    /**
     * Ensure definitions are loaded
     * @private
     */
    _ensureLoaded() {
        if (!this.loaded) {
            throw new Error('Validation definitions not loaded. Call load() first.');
        }
    }
}

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ValidationDefinitionLoader;
}
