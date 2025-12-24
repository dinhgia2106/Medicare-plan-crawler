/**
 * Medicare Plan Crawler - Main Entry Point
 * 
 * Usage:
 *   npm start                    - Run full crawler for all zipcodes
 *   npm run test                 - Test with single zipcode (63101)
 *   node src/main.js --test --zipcode 60601  - Test with specific zipcode
 */

import { readFile } from 'fs/promises';
import { parse } from 'csv-parse/sync';
import { config } from './config.js';
import { createCrawler } from './crawler.js';
import { exportToJSON, exportToCSV, exportErrors } from './exporters.js';

/**
 * Parse command line arguments
 */
function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        test: false,
        zipcode: null,
        limit: null
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];

        if (arg === '--test' || arg === '-t') {
            options.test = true;
        } else if (arg === '--zipcode' || arg === '-z') {
            options.zipcode = args[++i];
        } else if (arg === '--limit' || arg === '-l') {
            options.limit = parseInt(args[++i], 10);
        }
    }

    return options;
}

/**
 * Read and parse the ZipCodes.csv file
 */
async function loadZipcodes() {
    console.log(`Loading zipcodes from: ${config.inputFile}`);

    const fileContent = await readFile(config.inputFile, 'utf-8');

    const records = parse(fileContent, {
        delimiter: ';',
        columns: true,
        skip_empty_lines: true,
        trim: true
    });

    // Map to consistent format
    const zipcodes = records.map(row => ({
        state: row.State || row.state,
        city: row.City || row.city,
        zipcode: row['Zip Code'] || row.zipcode || row.ZipCode
    })).filter(z => z.zipcode); // Filter out any empty entries

    console.log(`Loaded ${zipcodes.length} zipcodes`);

    return zipcodes;
}

/**
 * Create requests for the crawler
 */
function createRequests(zipcodes) {
    return zipcodes.map(z => ({
        url: config.baseUrl,
        uniqueKey: `zipcode-${z.zipcode}`,
        userData: z
    }));
}

/**
 * Main function
 */
async function main() {
    console.log('');
    console.log('╔' + '═'.repeat(58) + '╗');
    console.log('║' + ' '.repeat(15) + '🏥 Medicare Plan Crawler' + ' '.repeat(18) + '║');
    console.log('╠' + '═'.repeat(58) + '╣');
    console.log('║  Version: 1.0.0' + ' '.repeat(41) + '║');
    console.log('║  Started: ' + new Date().toLocaleString() + ' '.repeat(25 - new Date().toLocaleString().length + 11) + '║');
    console.log('╚' + '═'.repeat(58) + '╝');
    console.log('');

    const options = parseArgs();

    // Load zipcodes
    let zipcodes = await loadZipcodes();

    // Apply filters based on command line options
    if (options.test && options.zipcode) {
        // Test mode with specific zipcode
        zipcodes = zipcodes.filter(z => z.zipcode === options.zipcode);
        if (zipcodes.length === 0) {
            // If zipcode not in file, create a test entry
            zipcodes = [{ state: 'Test', city: 'Test', zipcode: options.zipcode }];
        }
        console.log(`Test mode: Using zipcode ${options.zipcode}`);
    } else if (options.test) {
        // Test mode with first zipcode
        zipcodes = zipcodes.slice(0, 1);
        console.log(`Test mode: Using first zipcode ${zipcodes[0]?.zipcode}`);
    } else if (options.limit) {
        // Limit number of zipcodes
        zipcodes = zipcodes.slice(0, options.limit);
        console.log(`Limited to ${options.limit} zipcodes`);
    }

    console.log(`\nProcessing ${zipcodes.length} zipcode(s)...`);
    console.log('-'.repeat(60));

    // Create crawler
    const crawler = createCrawler(zipcodes);

    // Create requests
    const requests = createRequests(zipcodes);

    // Run the crawler
    const startTime = Date.now();

    try {
        await crawler.run(requests);
    } catch (err) {
        console.error('Crawler error:', err.message);
    }

    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

    console.log('');
    console.log('╔' + '═'.repeat(58) + '╗');
    console.log('║' + ' '.repeat(18) + '🎉 CRAWLING COMPLETE!' + ' '.repeat(19) + '║');
    console.log('╠' + '═'.repeat(58) + '╣');
    console.log(`║  ⏱️  Duration: ${duration} minutes` + ' '.repeat(Math.max(0, 41 - duration.length - 10)) + '║');
    console.log(`║  📋 Total Plans Extracted: ${crawler.allResults?.length || 0}` + ' '.repeat(Math.max(0, 30 - String(crawler.allResults?.length || 0).length)) + '║');
    console.log(`║  ❌ Errors: ${crawler.errors?.length || 0}` + ' '.repeat(Math.max(0, 44 - String(crawler.errors?.length || 0).length)) + '║');
    console.log('╚' + '═'.repeat(58) + '╝');

    // Export results
    if (crawler.allResults && crawler.allResults.length > 0) {
        console.log('\nExporting results...');
        await exportToJSON(crawler.allResults, 'medicare_plans.json');
        await exportToCSV(crawler.allResults, 'medicare_plans.csv');
    }

    // Export errors if any
    if (crawler.errors && crawler.errors.length > 0) {
        await exportErrors(crawler.errors, 'errors.json');
    }

    console.log(`\nFinished at: ${new Date().toISOString()}`);
}

// Run main function
main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
