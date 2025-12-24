/**
 * Data export utilities for JSON and CSV formats
 */

import { writeFile, mkdir } from 'fs/promises';
import { existsSync } from 'fs';
import { stringify } from 'csv-stringify/sync';
import { config } from './config.js';

/**
 * Ensure the output directory exists
 */
async function ensureOutputDir() {
    if (!existsSync(config.outputDir)) {
        await mkdir(config.outputDir, { recursive: true });
    }
}

/**
 * Export data to JSON file
 * @param {Array|Object} data - Data to export
 * @param {string} filename - Output filename (without path)
 */
export async function exportToJSON(data, filename = 'medicare_plans.json') {
    await ensureOutputDir();
    const filepath = `${config.outputDir}/${filename}`;

    const jsonContent = JSON.stringify(data, null, 2);
    await writeFile(filepath, jsonContent, 'utf-8');

    console.log(`Exported JSON to: ${filepath}`);
    return filepath;
}

/**
 * Flatten a nested object for CSV export
 * @param {Object} obj - Object to flatten
 * @param {string} prefix - Key prefix for nested properties
 * @returns {Object} Flattened object
 */
function flattenObject(obj, prefix = '') {
    const flattened = {};

    for (const [key, value] of Object.entries(obj)) {
        const newKey = prefix ? `${prefix}_${key}` : key;

        if (value === null || value === undefined) {
            flattened[newKey] = '';
        } else if (Array.isArray(value)) {
            flattened[newKey] = value.join('; ');
        } else if (typeof value === 'object') {
            Object.assign(flattened, flattenObject(value, newKey));
        } else {
            flattened[newKey] = value;
        }
    }

    return flattened;
}

/**
 * Export data to CSV file
 * @param {Array} data - Array of objects to export
 * @param {string} filename - Output filename (without path)
 */
export async function exportToCSV(data, filename = 'medicare_plans.csv') {
    await ensureOutputDir();
    const filepath = `${config.outputDir}/${filename}`;

    if (!Array.isArray(data) || data.length === 0) {
        console.warn('No data to export to CSV');
        return null;
    }

    // Flatten all objects
    const flattenedData = data.map(item => flattenObject(item));

    // Get all unique columns
    const columns = [...new Set(flattenedData.flatMap(Object.keys))];

    // Generate CSV
    const csvContent = stringify(flattenedData, {
        header: true,
        columns: columns
    });

    await writeFile(filepath, csvContent, 'utf-8');

    console.log(`Exported CSV to: ${filepath}`);
    return filepath;
}

/**
 * Export error log for failed zipcodes
 * @param {Array} errors - Array of error objects
 * @param {string} filename - Output filename
 */
export async function exportErrors(errors, filename = 'errors.json') {
    await ensureOutputDir();
    const filepath = `${config.outputDir}/${filename}`;

    const jsonContent = JSON.stringify(errors, null, 2);
    await writeFile(filepath, jsonContent, 'utf-8');

    console.log(`Exported errors to: ${filepath}`);
    return filepath;
}

/**
 * Append data to existing JSON file (for incremental saves)
 * @param {Object} planData - Single plan data to append
 * @param {string} filename - Output filename
 */
export async function appendToJSON(planData, filename = 'medicare_plans_incremental.json') {
    await ensureOutputDir();
    const filepath = `${config.outputDir}/${filename}`;

    let existingData = [];

    try {
        if (existsSync(filepath)) {
            const content = await import('fs').then(fs =>
                fs.readFileSync(filepath, 'utf-8')
            );
            existingData = JSON.parse(content);
        }
    } catch {
        existingData = [];
    }

    existingData.push(planData);

    await writeFile(filepath, JSON.stringify(existingData, null, 2), 'utf-8');

    return filepath;
}
