/**
 * Parallel Zipcode Processor
 * 
 * Runs 10 crawlers in parallel, each processing a batch of zipcodes.
 * Each crawler runs in its own browser instance with isolated storage.
 * 
 * Usage:
 *   node src/parallel.js                    - Run all zipcodes with 10 parallel workers
 *   node src/parallel.js --workers 5        - Run with 5 parallel workers
 *   node src/parallel.js --limit 20         - Process first 20 zipcodes only
 */

import { readFile, mkdir, appendFile, rm } from 'fs/promises';
import { parse } from 'csv-parse/sync';
import { PlaywrightCrawler, Configuration } from 'crawlee';
import { config, sleep } from './config.js';
import { extractPlanList, extractPlanDetails, getTotalPlanInfo } from './extractors.js';
import { appendToJSON, exportToJSON, exportToCSV, exportErrors } from './exporters.js';
import { existsSync } from 'fs';

const DEFAULT_WORKERS = 10;

/**
 * Parse command line arguments
 */
function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        workers: DEFAULT_WORKERS,
        limit: null
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        if (arg === '--workers' || arg === '-w') {
            options.workers = parseInt(args[++i], 10) || DEFAULT_WORKERS;
        } else if (arg === '--limit' || arg === '-l') {
            options.limit = parseInt(args[++i], 10);
        }
    }

    return options;
}

/**
 * Clean up worker storage directories
 */
async function cleanupWorkerStorage() {
    const storageDir = './storage';
    for (let i = 0; i < 20; i++) {
        const workerDir = `${storageDir}/worker_${i}`;
        if (existsSync(workerDir)) {
            await rm(workerDir, { recursive: true, force: true }).catch(() => { });
        }
    }
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

    const zipcodes = records.map(row => ({
        state: row.State || row.state,
        city: row.City || row.city,
        zipcode: row['Zip Code'] || row.zipcode || row.ZipCode
    })).filter(z => z.zipcode);

    console.log(`Loaded ${zipcodes.length} zipcodes`);
    return zipcodes;
}

/**
 * Split array into chunks
 */
function chunkArray(array, numChunks) {
    const chunks = Array.from({ length: numChunks }, () => []);
    array.forEach((item, index) => {
        chunks[index % numChunks].push(item);
    });
    return chunks.filter(chunk => chunk.length > 0);
}

// Longer delays for parallel mode to reduce memory pressure
const PARALLEL_DELAYS = {
    afterPageLoad: 4000,     // 4 seconds after page load
    betweenActions: 2000,    // 2 seconds between actions
    afterClick: 3000,        // 3 seconds after clicking
    workerStagger: 5000      // 5 seconds between worker starts
};

/**
 * Navigate through the Medicare wizard steps (with longer delays for parallel mode)
 */
async function navigateWizard(page, zipcode) {
    console.log(`  [${zipcode}] Navigating wizard...`);

    try {
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        // Step 1: Enter zipcode
        console.log(`  [${zipcode}] Step 1: Entering zipcode...`);
        const zipcodeInput = await page.waitForSelector(
            'input[name*="zip"], input[id*="zip"], #zip-code',
            { timeout: config.timeouts.element }
        );
        await zipcodeInput.click({ clickCount: 3 });
        await zipcodeInput.fill(zipcode);
        await sleep(PARALLEL_DELAYS.betweenActions);

        // Step 1.5: Check if county selection is required
        console.log(`  [${zipcode}] Step 1.5: Checking for county selection...`);
        await sleep(1000); // Wait for county options to appear
        
        const countySelectors = [
            'input[name="county"][type="radio"]',
            '[data-testid*="coverage-selector-fips"]',
            '.mct-c-coverage-selector-v2__county-choice input[type="radio"]'
        ];

        for (const selector of countySelectors) {
            try {
                const countyOptions = await page.$$(selector);
                if (countyOptions.length > 0) {
                    // Check if any is already selected
                    let alreadySelected = false;
                    for (const option of countyOptions) {
                        const isChecked = await option.isChecked();
                        if (isChecked) {
                            alreadySelected = true;
                            break;
                        }
                    }
                    
                    // If none selected, select the first one
                    if (!alreadySelected) {
                        const firstOption = countyOptions[0];
                        await firstOption.click();
                        console.log(`  [${zipcode}] Selected first county (${countyOptions.length} options)`);
                    }
                    break;
                }
            } catch (e) {
                continue;
            }
        }

        await sleep(PARALLEL_DELAYS.betweenActions);

        // Step 2: Click Continue
        console.log(`  [${zipcode}] Step 2: Clicking Continue...`);
        await clickContinueButton(page);
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        // Step 3: Select Medicare Advantage Plan (Part C)
        console.log(`  [${zipcode}] Step 3: Selecting plan type...`);
        const planTypeSelectors = [
            '#what-coverage-mapd',
            'input[value="MEDICARE_ADVANTAGE_PLAN"]',
            'label[for="what-coverage-mapd"]'
        ];

        for (const selector of planTypeSelectors) {
            try {
                const element = await page.$(selector);
                if (element && await element.isVisible()) {
                    await element.click();
                    break;
                }
            } catch { continue; }
        }
        await sleep(PARALLEL_DELAYS.betweenActions);

        // Step 4: Click "Find Plans"
        console.log(`  [${zipcode}] Step 4: Clicking Find Plans...`);
        const findPlansSelectors = [
            'button:has-text("Find Plans")',
            'a:has-text("Find Plans")'
        ];

        for (const selector of findPlansSelectors) {
            try {
                const button = await page.$(selector);
                if (button && await button.isVisible()) {
                    await button.click();
                    break;
                }
            } catch { continue; }
        }
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        // Step 5: Handle subsidy question
        console.log(`  [${zipcode}] Step 5: Handling subsidy question...`);
        const noHelpSelectors = ['input[value="noSubsidy"]', 'input[value="no"]'];
        for (const selector of noHelpSelectors) {
            try {
                const element = await page.$(selector);
                if (element && await element.isVisible()) {
                    await element.click();
                    break;
                }
            } catch { continue; }
        }
        await sleep(PARALLEL_DELAYS.betweenActions);
        await clickContinueButton(page);
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        // Step 6: Handle drug coverage question
        console.log(`  [${zipcode}] Step 6: Handling drug question...`);
        for (const selector of noHelpSelectors) {
            try {
                const element = await page.$(selector);
                if (element && await element.isVisible()) {
                    await element.click();
                    break;
                }
            } catch { continue; }
        }
        await sleep(PARALLEL_DELAYS.betweenActions);
        await clickContinueButton(page);
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        // Step 7: Skip Adding Providers
        console.log(`  [${zipcode}] Step 7: Skipping providers...`);
        const skipSelectors = [
            '[data-testid="continue-to-plans"]',
            'button:has-text("Skip Adding Providers")',
            'button:has-text("Skip")'
        ];
        for (const selector of skipSelectors) {
            try {
                const element = await page.$(selector);
                if (element && await element.isVisible()) {
                    await element.click();
                    break;
                }
            } catch { continue; }
        }
        await clickContinueButton(page);
        await sleep(PARALLEL_DELAYS.afterPageLoad);

        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        console.log(`  [${zipcode}] Wizard completed!`);
        return true;

    } catch (err) {
        console.error(`  [${zipcode}] Wizard failed: ${err.message}`);
        return false;
    }
}

/**
 * Helper function to click Continue button
 */
async function clickContinueButton(page) {
    const selectors = [
        'button:has-text("Continue")',
        'button:has-text("Next")',
        'a:has-text("Continue")',
        'button[type="submit"]'
    ];

    for (const selector of selectors) {
        try {
            const button = await page.$(selector);
            if (button && await button.isVisible()) {
                await button.click();
                await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                return true;
            }
        } catch { continue; }
    }
    return false;
}

/**
 * Click pagination button
 */
async function clickPaginationButton(page, targetPage) {
    try {
        const selectors = [
            `button[aria-label*="Page ${targetPage}"]`,
            `.ds-c-pagination__item button:has-text("${targetPage}")`
        ];

        for (const selector of selectors) {
            try {
                const button = await page.$(selector);
                if (button && await button.isVisible()) {
                    await button.click();
                    await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                    return true;
                }
            } catch { continue; }
        }
        return false;
    } catch {
        return false;
    }
}

/**
 * Extract all plans for a single zipcode
 */
async function extractAllPlans(page, zipcodeInfo) {
    const allPlans = [];
    const planSummaries = [];
    const processedPlanIds = new Set();

    const { totalPlans, totalPages } = await getTotalPlanInfo(page);
    console.log(`  [${zipcodeInfo.zipcode}] Found ${totalPlans} plans on ${totalPages} pages`);

    // Phase 1: Collect all plan URLs
    for (let p = 1; p <= totalPages; p++) {
        if (p > 1) {
            await clickPaginationButton(page, p);
            await page.waitForSelector(config.selectors.planCards, { timeout: 15000 }).catch(() => { });
            await sleep(1000);
        }

        const planList = await extractPlanList(page);
        for (const plan of planList) {
            const planId = plan.planId || `unknown-${p}-${planList.indexOf(plan)}`;
            if (!processedPlanIds.has(planId)) {
                processedPlanIds.add(planId);
                planSummaries.push(plan);
            }
        }

        if (planSummaries.length >= totalPlans) break;
    }

    // Phase 2: Extract details for each plan
    for (let i = 0; i < planSummaries.length; i++) {
        const plan = planSummaries[i];

        try {
            if (plan.detailsUrl) {
                await page.goto(plan.detailsUrl, { waitUntil: 'domcontentloaded' });
                await page.waitForSelector('.PlanDetailsPagePlanInfo, .e2e-plan-details-page', { timeout: 15000 }).catch(() => { });
                await sleep(1000);

                const details = await extractPlanDetails(page);
                const fullPlanData = {
                    ...zipcodeInfo,
                    ...plan,
                    details,
                    scrapedAt: new Date().toISOString()
                };

                allPlans.push(fullPlanData);
                await appendToJSON(fullPlanData);
            } else {
                allPlans.push({
                    ...zipcodeInfo,
                    ...plan,
                    details: null,
                    scrapedAt: new Date().toISOString()
                });
            }
        } catch (err) {
            allPlans.push({
                ...zipcodeInfo,
                ...plan,
                details: null,
                error: err.message,
                scrapedAt: new Date().toISOString()
            });
        }
    }

    const successCount = allPlans.filter(p => !p.error).length;
    console.log(`  [${zipcodeInfo.zipcode}] Completed: ${successCount}/${allPlans.length} plans`);

    return allPlans;
}

/**
 * Process a batch of zipcodes with a single crawler
 * Each worker uses isolated storage to avoid conflicts
 */
async function processBatch(batchIndex, zipcodes) {
    console.log(`\n[Worker ${batchIndex + 1}] Starting with ${zipcodes.length} zipcodes`);

    const allResults = [];
    const errors = [];

    // Create isolated configuration for this worker
    const workerStorageDir = `./storage/worker_${batchIndex}`;
    await mkdir(workerStorageDir, { recursive: true }).catch(() => { });

    const workerConfig = new Configuration({
        storageClientOptions: {
            localDataDirectory: workerStorageDir
        },
        persistStateIntervalMillis: 60000, // Reduce state persistence frequency
    });

    const crawler = new PlaywrightCrawler({
        maxConcurrency: 1,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: config.requestHandlerTimeoutSecs,

        launchContext: {
            launchOptions: {
                headless: config.headless,
                slowMo: config.slowMo
            }
        },

        async requestHandler({ page, request, log }) {
            const zipcodeInfo = request.userData;
            const { zipcode, state, city } = zipcodeInfo;

            log.info(`[Worker ${batchIndex + 1}] Processing: ${zipcode} (${city}, ${state})`);

            try {
                const wizardSuccess = await navigateWizard(page, zipcode);
                if (!wizardSuccess) {
                    throw new Error('Failed to navigate wizard');
                }

                const plans = await extractAllPlans(page, zipcodeInfo);
                log.info(`[Worker ${batchIndex + 1}] Extracted ${plans.length} plans for ${zipcode}`);

                // Store in local array instead of shared Dataset
                allResults.push(...plans);

            } catch (err) {
                log.error(`[Worker ${batchIndex + 1}] Error for ${zipcode}: ${err.message}`);
                errors.push({
                    zipcode,
                    state,
                    city,
                    error: err.message,
                    timestamp: new Date().toISOString()
                });
            }

            await sleep(config.delays.betweenZipcodes);
        },

        async failedRequestHandler({ request, log }) {
            const { zipcode, state, city } = request.userData;
            log.error(`[Worker ${batchIndex + 1}] Failed after retries: ${zipcode}`);
            errors.push({
                zipcode,
                state,
                city,
                error: 'Max retries exceeded',
                timestamp: new Date().toISOString()
            });
        }
    }, workerConfig);

    const requests = zipcodes.map(z => ({
        url: config.baseUrl,
        uniqueKey: `worker-${batchIndex}-zipcode-${z.zipcode}`,
        userData: z
    }));

    try {
        await crawler.run(requests);
    } catch (err) {
        console.error(`[Worker ${batchIndex + 1}] Crawler error: ${err.message}`);
    }

    console.log(`[Worker ${batchIndex + 1}] Finished: ${allResults.length} plans, ${errors.length} errors`);

    return { results: allResults, errors };
}

/**
 * Main function - Run parallel workers
 */
async function main() {
    console.log('\n' + '='.repeat(70));
    console.log('Medicare Plan Crawler - PARALLEL MODE');
    console.log('Started: ' + new Date().toLocaleString());
    console.log('='.repeat(70));

    const options = parseArgs();
    console.log(`Configuration: ${options.workers} workers`);

    // Ensure output directory exists
    if (!existsSync(config.outputDir)) {
        await mkdir(config.outputDir, { recursive: true });
    }

    // Load zipcodes
    let zipcodes = await loadZipcodes();

    if (options.limit) {
        zipcodes = zipcodes.slice(0, options.limit);
        console.log(`Limited to ${options.limit} zipcodes`);
    }

    // Split zipcodes into batches for parallel processing
    const batches = chunkArray(zipcodes, options.workers);
    console.log(`\nSplit ${zipcodes.length} zipcodes into ${batches.length} batches`);
    batches.forEach((batch, i) => {
        console.log(`  Worker ${i + 1}: ${batch.length} zipcodes (${batch[0]?.zipcode} - ${batch[batch.length - 1]?.zipcode})`);
    });

    const startTime = Date.now();

    // Run all workers in parallel with staggered starts
    console.log('\n' + '-'.repeat(70));
    console.log('Starting parallel workers with staggered delays...');
    console.log('-'.repeat(70));

    // Stagger worker starts to reduce initial memory spike
    const workerPromises = batches.map(async (batch, index) => {
        // Wait before starting this worker (stagger by 5 seconds each)
        const staggerDelay = index * 5000;
        if (staggerDelay > 0) {
            console.log(`[Worker ${index + 1}] Waiting ${staggerDelay / 1000}s before start...`);
            await sleep(staggerDelay);
        }
        return processBatch(index, batch);
    });
    const workerResults = await Promise.all(workerPromises);

    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

    // Aggregate results
    const allResults = workerResults.flatMap(w => w.results);
    const allErrors = workerResults.flatMap(w => w.errors);

    console.log('\n' + '='.repeat(70));
    console.log('PARALLEL CRAWLING COMPLETE');
    console.log('='.repeat(70));
    console.log(`Duration: ${duration} minutes`);
    console.log(`Total Plans: ${allResults.length}`);
    console.log(`Total Errors: ${allErrors.length}`);
    console.log(`Workers Used: ${batches.length}`);

    // Export final aggregated results
    if (allResults.length > 0) {
        console.log('\nExporting aggregated results...');
        await exportToJSON(allResults, 'medicare_plans_parallel.json');
        await exportToCSV(allResults, 'medicare_plans_parallel.csv');
    }

    if (allErrors.length > 0) {
        await exportErrors(allErrors, 'errors_parallel.json');
    }

    console.log(`\nFinished at: ${new Date().toISOString()}`);
}

// Run the parallel crawler
main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
