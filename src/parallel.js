/**
 * Parallel Zipcode Processor - RESUMABLE & TWO-PHASE
 * 
 * Features:
 *   - RESUME: Skip already completed zipcodes when re-running
 *   - ORDER PRESERVED: Maintains CSV order throughout
 *   - INCREMENTAL SAVE: Saves after every change to prevent data loss
 *   - TWO-PHASE: Phase 1 collects all URLs, Phase 2 fills details
 * 
 * Usage:
 *   node src/parallel.js                    - Run with default 4 workers
 *   node src/parallel.js --workers 10       - Run with 10 workers
 *   node src/parallel.js --limit 20         - Process first 20 zipcodes only
 *   node src/parallel.js --reset            - Reset and start fresh
 */

import { readFile, writeFile, mkdir } from 'fs/promises';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { PlaywrightCrawler } from 'crawlee';
import { config, sleep } from './config.js';
import { extractPlanList, extractPlanDetails, getTotalPlanInfo } from './extractors.js';
import { exportErrors } from './exporters.js';
import { existsSync } from 'fs';

const DEFAULT_WORKERS = 4;
const STATE_FILE = 'crawler_state.json';
const OUTPUT_JSON = 'medicare_plans.json';
const OUTPUT_CSV_SUMMARY = 'medicare_plans_summary.csv';
const OUTPUT_CSV_PLANS = 'medicare_plans_details.csv';

// ============================================================================
// STATUS CONSTANTS
// ============================================================================

const ZIPCODE_STATUS = {
    PENDING: 'pending',
    URLS_COLLECTED: 'urls_collected',  // Phase 1 done
    COMPLETED: 'completed',             // Phase 2 done (all details filled)
    ERROR: 'error'
};

const PLAN_STATUS = {
    PENDING: 'pending',
    COMPLETED: 'completed',
    ERROR: 'error'
};

// ============================================================================
// GLOBAL STATE - In-memory structure with disk persistence
// ============================================================================

let state = {
    metadata: {
        createdAt: null,
        lastUpdatedAt: null,
        totalZipcodes: 0,
        phase1Completed: 0,  // URLs collected
        phase2Completed: 0,  // Details filled
        totalPlansFound: 0,
        totalPlansFilled: 0,
        workers: 0
    },
    // Array to preserve order from CSV
    zipcodeOrder: [],  // Array of zipcode strings in original order
    // Map for quick access
    zipcodes: {}  // { zipcode: ZipcodeEntry }
};

// ============================================================================
// STATE MANAGEMENT
// ============================================================================

/**
 * Create a new zipcode entry
 */
function createZipcodeEntry(zipcodeInfo, index) {
    return {
        index,  // Original position from CSV
        zipcode: zipcodeInfo.zipcode,
        state: zipcodeInfo.state,
        city: zipcodeInfo.city,
        status: ZIPCODE_STATUS.PENDING,
        phase1StartedAt: null,
        phase1CompletedAt: null,
        phase2StartedAt: null,
        phase2CompletedAt: null,
        totalPlans: 0,
        plansWithDetails: 0,
        error: null,
        plans: []  // Array of PlanEntry
    };
}

/**
 * Create a new plan entry
 */
function createPlanEntry(planSummary) {
    return {
        status: PLAN_STATUS.PENDING,
        planId: planSummary.planId || null,
        planName: planSummary.planName || null,
        planType: planSummary.planType || null,
        monthlyPremium: planSummary.monthlyPremium || null,
        estimatedAnnualCost: planSummary.estimatedAnnualCost || null,
        starRating: planSummary.starRating || null,
        detailsUrl: planSummary.detailsUrl || null,
        details: null,  // Will be filled in Phase 2
        error: null,
        scrapedAt: null
    };
}

/**
 * Load state from disk if exists
 */
async function loadState() {
    const statePath = `${config.outputDir}/${STATE_FILE}`;
    
    if (existsSync(statePath)) {
        try {
            const content = await readFile(statePath, 'utf-8');
            const loaded = JSON.parse(content);
            state = loaded;
            console.log(`📂 Loaded existing state from ${statePath}`);
            console.log(`   - Phase 1 (URLs): ${state.metadata.phase1Completed}/${state.metadata.totalZipcodes} zipcodes`);
            console.log(`   - Phase 2 (Details): ${state.metadata.totalPlansFilled}/${state.metadata.totalPlansFound} plans`);
            return true;
        } catch (err) {
            console.warn(`⚠️ Could not load state: ${err.message}`);
            return false;
        }
    }
    return false;
}

/**
 * Save state to disk (call after every important change)
 */
async function saveState() {
    const statePath = `${config.outputDir}/${STATE_FILE}`;
    state.metadata.lastUpdatedAt = new Date().toISOString();
    await writeFile(statePath, JSON.stringify(state, null, 2), 'utf-8');
}

/**
 * Initialize fresh state from zipcodes
 */
function initializeState(zipcodes, workers) {
    state = {
        metadata: {
            createdAt: new Date().toISOString(),
            lastUpdatedAt: new Date().toISOString(),
            totalZipcodes: zipcodes.length,
            phase1Completed: 0,
            phase2Completed: 0,
            totalPlansFound: 0,
            totalPlansFilled: 0,
            workers
        },
        zipcodeOrder: zipcodes.map(z => z.zipcode),
        zipcodes: {}
    };
    
    zipcodes.forEach((z, index) => {
        state.zipcodes[z.zipcode] = createZipcodeEntry(z, index);
    });
    
    console.log(`\n📋 Initialized fresh state with ${zipcodes.length} zipcodes`);
}

/**
 * Merge new zipcodes with existing state (preserve completed ones)
 */
function mergeState(zipcodes, workers) {
    // Update order from new CSV
    const newOrder = zipcodes.map(z => z.zipcode);
    
    // Keep existing completed data, add new zipcodes
    const existingZipcodes = { ...state.zipcodes };
    
    state.zipcodeOrder = newOrder;
    state.metadata.totalZipcodes = zipcodes.length;
    state.metadata.workers = workers;
    
    // Recount stats
    let phase1Done = 0;
    let phase2Done = 0;
    let totalPlans = 0;
    let filledPlans = 0;
    
    zipcodes.forEach((z, index) => {
        if (existingZipcodes[z.zipcode]) {
            // Keep existing entry but update index
            state.zipcodes[z.zipcode] = existingZipcodes[z.zipcode];
            state.zipcodes[z.zipcode].index = index;
            
            const entry = state.zipcodes[z.zipcode];
            if (entry.status === ZIPCODE_STATUS.URLS_COLLECTED || entry.status === ZIPCODE_STATUS.COMPLETED) {
                phase1Done++;
                totalPlans += entry.totalPlans;
            }
            if (entry.status === ZIPCODE_STATUS.COMPLETED) {
                phase2Done++;
                filledPlans += entry.plansWithDetails;
            } else {
                // Count filled plans for in-progress zipcodes
                filledPlans += entry.plans.filter(p => p.status === PLAN_STATUS.COMPLETED).length;
            }
        } else {
            // New zipcode
            state.zipcodes[z.zipcode] = createZipcodeEntry(z, index);
        }
    });
    
    state.metadata.phase1Completed = phase1Done;
    state.metadata.phase2Completed = phase2Done;
    state.metadata.totalPlansFound = totalPlans;
    state.metadata.totalPlansFilled = filledPlans;
    
    console.log(`\n📋 Merged state: ${phase1Done} URLs collected, ${filledPlans}/${totalPlans} plans detailed`);
}

// ============================================================================
// PROGRESS & OUTPUT
// ============================================================================

/**
 * Get progress summary
 */
function getProgress() {
    const total = state.metadata.totalZipcodes;
    const phase1 = state.metadata.phase1Completed;
    const phase2 = state.metadata.phase2Completed;
    const plansFound = state.metadata.totalPlansFound;
    const plansFilled = state.metadata.totalPlansFilled;
    
    const pending = Object.values(state.zipcodes).filter(z => z.status === ZIPCODE_STATUS.PENDING).length;
    const errors = Object.values(state.zipcodes).filter(z => z.status === ZIPCODE_STATUS.ERROR).length;
    
    return {
        total,
        phase1,
        phase2,
        pending,
        errors,
        plansFound,
        plansFilled,
        phase1Pct: total > 0 ? ((phase1 / total) * 100).toFixed(1) : 0,
        phase2Pct: plansFound > 0 ? ((plansFilled / plansFound) * 100).toFixed(1) : 0
    };
}

/**
 * Create ASCII progress bar
 */
function progressBar(current, total, width = 25) {
    if (total === 0) return '[' + '░'.repeat(width) + ']';
    const filled = Math.round((current / total) * width);
    const empty = width - filled;
    return `[${'█'.repeat(filled)}${'░'.repeat(empty)}]`;
}

/**
 * Print progress
 */
function printProgress(phase, zipcode, action) {
    const p = getProgress();
    
    if (phase === 1) {
        console.log(`\n📍 Phase 1 ${progressBar(p.phase1, p.total)} ${p.phase1Pct}% | ${p.phase1}/${p.total} URLs | 📊 ${p.plansFound} plans found`);
    } else {
        console.log(`\n📋 Phase 2 ${progressBar(p.plansFilled, p.plansFound)} ${p.phase2Pct}% | ${p.plansFilled}/${p.plansFound} details`);
    }
    
    if (zipcode && action) {
        console.log(`   ${action}: ${zipcode}`);
    }
}

/**
 * Export outputs (JSON + CSVs) - call frequently for incremental saves
 */
async function exportOutputs() {
    const outputDir = config.outputDir;
    
    // Prepare data in original CSV order
    const orderedEntries = state.zipcodeOrder.map(z => state.zipcodes[z]).filter(Boolean);
    
    // JSON output - full data
    const jsonData = orderedEntries.map(entry => ({
        index: entry.index,
        zipcode: entry.zipcode,
        state: entry.state,
        city: entry.city,
        status: entry.status,
        totalPlans: entry.totalPlans,
        plansWithDetails: entry.plansWithDetails,
        error: entry.error,
        plans: entry.plans
    }));
    
    const jsonPath = `${outputDir}/${OUTPUT_JSON}`;
    await writeFile(jsonPath, JSON.stringify(jsonData, null, 2), 'utf-8');
    
    // Summary CSV - one row per zipcode
    const summaryData = orderedEntries.map(entry => ({
        index: entry.index,
        zipcode: entry.zipcode,
        state: entry.state,
        city: entry.city,
        status: entry.status,
        total_plans: entry.totalPlans,
        plans_with_details: entry.plansWithDetails,
        error: entry.error || '',
        plans_json: JSON.stringify(entry.plans)
    }));
    
    const summaryPath = `${outputDir}/${OUTPUT_CSV_SUMMARY}`;
    await writeFile(summaryPath, stringify(summaryData, { header: true }), 'utf-8');
    
    // Details CSV - one row per plan
    const detailsData = [];
    for (const entry of orderedEntries) {
        if (entry.plans.length === 0) {
            detailsData.push({
                zipcode_index: entry.index,
                zipcode: entry.zipcode,
                state: entry.state,
                city: entry.city,
                zipcode_status: entry.status,
                plan_status: '',
                plan_id: '',
                plan_name: '',
                plan_type: '',
                monthly_premium: '',
                estimated_annual_cost: '',
                star_rating: '',
                details_url: '',
                details_json: '',
                error: entry.error || ''
            });
        } else {
            for (const plan of entry.plans) {
                detailsData.push({
                    zipcode_index: entry.index,
                    zipcode: entry.zipcode,
                    state: entry.state,
                    city: entry.city,
                    zipcode_status: entry.status,
                    plan_status: plan.status,
                    plan_id: plan.planId || '',
                    plan_name: plan.planName || '',
                    plan_type: plan.planType || '',
                    monthly_premium: plan.monthlyPremium || '',
                    estimated_annual_cost: plan.estimatedAnnualCost || '',
                    star_rating: plan.starRating || '',
                    details_url: plan.detailsUrl || '',
                    details_json: JSON.stringify(plan.details || {}),
                    error: plan.error || ''
                });
            }
        }
    }
    
    const detailsPath = `${outputDir}/${OUTPUT_CSV_PLANS}`;
    await writeFile(detailsPath, stringify(detailsData, { header: true }), 'utf-8');
    
    return { jsonPath, summaryPath, detailsPath };
}

// ============================================================================
// COMMAND LINE & FILE LOADING
// ============================================================================

function parseArgs() {
    const args = process.argv.slice(2);
    const options = {
        workers: DEFAULT_WORKERS,
        limit: null,
        reset: false
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        if (arg === '--workers' || arg === '-w') {
            options.workers = parseInt(args[++i], 10) || DEFAULT_WORKERS;
        } else if (arg === '--limit' || arg === '-l') {
            options.limit = parseInt(args[++i], 10);
        } else if (arg === '--reset' || arg === '-r') {
            options.reset = true;
        }
    }

    return options;
}

async function loadZipcodes() {
    console.log(`📂 Loading zipcodes from: ${config.inputFile}`);
    const fileContent = await readFile(config.inputFile, 'utf-8');
    
    const records = parse(fileContent, {
        delimiter: ';',
        columns: true,
        skip_empty_lines: true,
        trim: true
    });

    const zipcodes = records.map((row, idx) => ({
        index: idx,
        state: row.State || row.state,
        city: row.City || row.city,
        zipcode: row['Zip Code'] || row.zipcode || row.ZipCode
    })).filter(z => z.zipcode);

    console.log(`   Loaded ${zipcodes.length} zipcodes`);
    return zipcodes;
}

// ============================================================================
// NAVIGATION HELPERS
// ============================================================================

const DELAYS = {
    afterPageLoad: 4000,
    betweenActions: 2000,
    afterClick: 3000
};

async function navigateWizard(page, zipcode) {
    console.log(`  [${zipcode}] Navigating wizard...`);

    try {
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(DELAYS.afterPageLoad);

        // Step 1: Enter zipcode
        const zipcodeInput = await page.waitForSelector(
            'input[name*="zip"], input[id*="zip"], #zip-code',
            { timeout: config.timeouts.element }
        );
        await zipcodeInput.click({ clickCount: 3 });
        await zipcodeInput.fill(zipcode);
        await sleep(DELAYS.betweenActions);

        // Step 1.5: County selection
        await sleep(1000);
        const countySelectors = [
            'input[name="county"][type="radio"]',
            '[data-testid*="coverage-selector-fips"]',
            '.mct-c-coverage-selector-v2__county-choice input[type="radio"]'
        ];

        for (const selector of countySelectors) {
            try {
                const options = await page.$$(selector);
                if (options.length > 0) {
                    let selected = false;
                    for (const opt of options) {
                        if (await opt.isChecked()) { selected = true; break; }
                    }
                    if (!selected) await options[0].click();
                    break;
                }
            } catch { continue; }
        }

        await sleep(DELAYS.betweenActions);
        await clickContinue(page);
        await sleep(DELAYS.afterPageLoad);

        // Step 3: Select Medicare Advantage Plan
        const planTypeSelectors = ['#what-coverage-mapd', 'input[value="MEDICARE_ADVANTAGE_PLAN"]', 'label[for="what-coverage-mapd"]'];
        for (const sel of planTypeSelectors) {
            try {
                const el = await page.$(sel);
                if (el && await el.isVisible()) { await el.click(); break; }
            } catch { continue; }
        }
        await sleep(DELAYS.betweenActions);

        // Step 4: Find Plans
        const findSelectors = ['button:has-text("Find Plans")', 'a:has-text("Find Plans")'];
        for (const sel of findSelectors) {
            try {
                const btn = await page.$(sel);
                if (btn && await btn.isVisible()) { await btn.click(); break; }
            } catch { continue; }
        }
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(DELAYS.afterPageLoad);

        // Step 5: Medicare cost help - select "No"
        await page.waitForSelector('input[type="checkbox"], input[type="radio"]', { timeout: 10000 }).catch(() => {});
        await sleep(1000);
        
        const noHelpSelectors = [
            'input[value="none"]', 'input#none', 'label[for="none"]',
            'input[name="subsidyTypes"][value="none"]', 'input[type="radio"][value="none"]'
        ];
        for (const sel of noHelpSelectors) {
            try {
                const el = await page.$(sel);
                if (el && await el.isVisible()) { await el.click(); break; }
            } catch { continue; }
        }
        
        // Fallback: click label with "don't get help"
        try {
            await page.evaluate(() => {
                const labels = document.querySelectorAll('label');
                for (const l of labels) {
                    if (l.textContent.toLowerCase().includes("don't get help")) {
                        l.click(); return;
                    }
                }
            });
        } catch {}

        await sleep(DELAYS.betweenActions);
        await clickContinue(page);
        await sleep(DELAYS.afterPageLoad);

        // Step 6: Drug coverage - select "No"
        const noDrugSelectors = ['input[value="no"]', 'input[value="false"]', 'label:has-text("No")'];
        for (const sel of noDrugSelectors) {
            try {
                const el = await page.$(sel);
                if (el && await el.isVisible()) { await el.click(); break; }
            } catch { continue; }
        }
        await sleep(DELAYS.betweenActions);
        await clickContinue(page);
        await sleep(DELAYS.afterPageLoad);

        // Step 7: Skip providers
        const skipSelectors = ['[data-testid="continue-to-plans"]', 'button:has-text("Skip")'];
        for (const sel of skipSelectors) {
            try {
                const el = await page.$(sel);
                if (el && await el.isVisible()) { await el.click(); break; }
            } catch { continue; }
        }
        await clickContinue(page);
        await sleep(DELAYS.afterPageLoad);

        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        console.log(`  [${zipcode}] ✓ Wizard completed`);
        return true;

    } catch (err) {
        console.error(`  [${zipcode}] ✗ Wizard failed: ${err.message}`);
        return false;
    }
}

async function clickContinue(page) {
    const selectors = ['button:has-text("Continue")', 'button:has-text("Next")', 'a:has-text("Continue")', 'button[type="submit"]'];
    for (const sel of selectors) {
        try {
            const btn = await page.$(sel);
            if (btn && await btn.isVisible()) {
                await btn.click();
                await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                return true;
            }
        } catch { continue; }
    }
    return false;
}

async function clickPagination(page, targetPage) {
    try {
        const selectors = [
            `button[aria-label*="Page ${targetPage}"]`,
            `a[aria-label*="Page ${targetPage}"]`,
            `.ds-c-pagination__item button:has-text("${targetPage}")`
        ];

        for (const sel of selectors) {
            try {
                const btn = await page.$(sel);
                if (btn && await btn.isVisible()) {
                    await btn.click();
                    await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                    return true;
                }
            } catch { continue; }
        }

        // Try Next button
        const nextBtn = await page.$('.ds-c-pagination__item--next button, button:has-text("Next")');
        if (nextBtn && await nextBtn.isVisible()) {
            await nextBtn.click();
            await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
            return true;
        }

        return false;
    } catch {
        return false;
    }
}

async function isPageAlive(page) {
    try {
        await page.evaluate(() => true);
        return true;
    } catch {
        return false;
    }
}

// ============================================================================
// PHASE 1: COLLECT ALL PLAN URLs FOR A ZIPCODE
// ============================================================================

async function collectPlanUrls(page, zipcodeInfo) {
    const plans = [];
    const seenIds = new Set();

    const { totalPlans, totalPages } = await getTotalPlanInfo(page);
    console.log(`  [${zipcodeInfo.zipcode}] Found ${totalPlans} plans on ${totalPages} pages`);

    for (let p = 1; p <= totalPages; p++) {
        if (p > 1) {
            const clicked = await clickPagination(page, p);
            if (!clicked) {
                console.log(`  [${zipcodeInfo.zipcode}] Could not go to page ${p}, stopping`);
                break;
            }
            await page.waitForSelector(config.selectors.planCards, { timeout: 15000 }).catch(() => {});
            await sleep(1000);
        }

        const planList = await extractPlanList(page);
        
        for (const plan of planList) {
            const id = plan.planId || `unknown-${p}-${plans.length}`;
            if (!seenIds.has(id)) {
                seenIds.add(id);
                plans.push(createPlanEntry(plan));
            }
        }

        if (plans.length >= totalPlans) break;
    }

    console.log(`  [${zipcodeInfo.zipcode}] ✓ Collected ${plans.length} plan URLs`);
    return plans;
}

// ============================================================================
// PHASE 2: FILL PLAN DETAILS
// ============================================================================

async function fillPlanDetails(page, zipcode, planEntry) {
    if (!planEntry.detailsUrl) {
        planEntry.status = PLAN_STATUS.COMPLETED;
        planEntry.scrapedAt = new Date().toISOString();
        return planEntry;
    }

    try {
        await page.goto(planEntry.detailsUrl, { waitUntil: 'domcontentloaded' });
        await page.waitForSelector('.PlanDetailsPagePlanInfo, .e2e-plan-details-page', { timeout: 15000 }).catch(() => {});
        await sleep(1000);

        const details = await extractPlanDetails(page);
        planEntry.details = details;
        planEntry.status = PLAN_STATUS.COMPLETED;
        planEntry.scrapedAt = new Date().toISOString();

    } catch (err) {
        planEntry.status = PLAN_STATUS.ERROR;
        planEntry.error = err.message;
        planEntry.scrapedAt = new Date().toISOString();
    }

    return planEntry;
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function main() {
    console.log('\n' + '═'.repeat(70));
    console.log('Medicare Plan Crawler - RESUMABLE TWO-PHASE MODE');
    console.log('Started: ' + new Date().toLocaleString());
    console.log('═'.repeat(70));

    const options = parseArgs();
    console.log(`\n⚙️ Configuration:`);
    console.log(`   Workers: ${options.workers}`);
    console.log(`   Limit: ${options.limit || 'none'}`);
    console.log(`   Reset: ${options.reset}`);

    // Ensure output directory exists
    if (!existsSync(config.outputDir)) {
        await mkdir(config.outputDir, { recursive: true });
    }

    // Load zipcodes from CSV
    let zipcodes = await loadZipcodes();
    if (options.limit) {
        zipcodes = zipcodes.slice(0, options.limit);
        console.log(`   Limited to first ${options.limit} zipcodes`);
    }

    // Load or initialize state
    const stateExists = await loadState();
    
    if (options.reset || !stateExists) {
        initializeState(zipcodes, options.workers);
        await saveState();
        await exportOutputs();
        console.log(`\n📁 Fresh start - all outputs initialized`);
    } else {
        mergeState(zipcodes, options.workers);
        await saveState();
    }

    const startTime = Date.now();
    const errors = [];

    // ==========================================================================
    // PHASE 1: Collect all plan URLs for pending zipcodes
    // ==========================================================================
    console.log('\n' + '─'.repeat(70));
    console.log('📍 PHASE 1: Collecting Plan URLs');
    console.log('─'.repeat(70));

    // Get zipcodes that need Phase 1
    const phase1Pending = state.zipcodeOrder
        .map(z => state.zipcodes[z])
        .filter(z => z && z.status === ZIPCODE_STATUS.PENDING);

    if (phase1Pending.length === 0) {
        console.log('✓ All zipcodes already have URLs collected');
    } else {
        console.log(`Processing ${phase1Pending.length} zipcodes...`);

        const phase1Crawler = new PlaywrightCrawler({
            maxConcurrency: options.workers,
            maxRequestRetries: 3,
            requestHandlerTimeoutSecs: config.requestHandlerTimeoutSecs,
            useSessionPool: true,
            launchContext: {
                launchOptions: { headless: config.headless, slowMo: config.slowMo }
            },

            async requestHandler({ page, request, log }) {
                const { zipcode, state: st, city } = request.userData;
                
                state.zipcodes[zipcode].phase1StartedAt = new Date().toISOString();
                printProgress(1, zipcode, '🔄 Collecting URLs');

                try {
                    const wizardOk = await navigateWizard(page, zipcode);
                    if (!wizardOk) throw new Error('Wizard navigation failed');

                    const plans = await collectPlanUrls(page, { zipcode, state: st, city });
                    
                    // Update state
                    state.zipcodes[zipcode].plans = plans;
                    state.zipcodes[zipcode].totalPlans = plans.length;
                    state.zipcodes[zipcode].status = ZIPCODE_STATUS.URLS_COLLECTED;
                    state.zipcodes[zipcode].phase1CompletedAt = new Date().toISOString();
                    state.metadata.phase1Completed++;
                    state.metadata.totalPlansFound += plans.length;

                    // SAVE IMMEDIATELY
                    await saveState();
                    await exportOutputs();

                    printProgress(1, zipcode, `✅ Got ${plans.length} plans`);

                } catch (err) {
                    log.error(`[${zipcode}] Error: ${err.message}`);
                    
                    state.zipcodes[zipcode].status = ZIPCODE_STATUS.ERROR;
                    state.zipcodes[zipcode].error = err.message;
                    state.zipcodes[zipcode].phase1CompletedAt = new Date().toISOString();
                    state.metadata.phase1Completed++;

                    errors.push({ zipcode, state: st, city, phase: 1, error: err.message });
                    
                    await saveState();
                    await exportOutputs();

                    printProgress(1, zipcode, `❌ Error: ${err.message.substring(0, 40)}`);
                }

                await sleep(2000);
            },

            async failedRequestHandler({ request }) {
                const { zipcode, state: st, city } = request.userData;
                
                state.zipcodes[zipcode].status = ZIPCODE_STATUS.ERROR;
                state.zipcodes[zipcode].error = 'Max retries exceeded';
                state.metadata.phase1Completed++;
                
                errors.push({ zipcode, state: st, city, phase: 1, error: 'Max retries exceeded' });
                
                await saveState();
                await exportOutputs();
            }
        });

        const phase1Requests = phase1Pending.map(z => ({
            url: config.baseUrl,
            uniqueKey: `phase1-${z.zipcode}`,
            userData: { zipcode: z.zipcode, state: z.state, city: z.city }
        }));

        await phase1Crawler.run(phase1Requests);
    }

    // ==========================================================================
    // PHASE 2: Fill details for all plans
    // ==========================================================================
    console.log('\n' + '─'.repeat(70));
    console.log('📋 PHASE 2: Filling Plan Details');
    console.log('─'.repeat(70));

    // Get all plans that need details
    const plansToFill = [];
    for (const zipcode of state.zipcodeOrder) {
        const entry = state.zipcodes[zipcode];
        if (!entry || entry.status === ZIPCODE_STATUS.ERROR || entry.status === ZIPCODE_STATUS.PENDING) continue;
        
        for (let i = 0; i < entry.plans.length; i++) {
            const plan = entry.plans[i];
            if (plan.status === PLAN_STATUS.PENDING) {
                plansToFill.push({ zipcode, planIndex: i, plan, zipcodeEntry: entry });
            }
        }
    }

    if (plansToFill.length === 0) {
        console.log('✓ All plans already have details filled');
    } else {
        console.log(`Processing ${plansToFill.length} plans...`);

        const phase2Crawler = new PlaywrightCrawler({
            maxConcurrency: options.workers,
            maxRequestRetries: 2,
            requestHandlerTimeoutSecs: 120,
            useSessionPool: true,
            launchContext: {
                launchOptions: { headless: config.headless, slowMo: config.slowMo }
            },

            async requestHandler({ page, request, log }) {
                const { zipcode, planIndex, planId } = request.userData;
                
                const entry = state.zipcodes[zipcode];
                if (!entry || !entry.plans[planIndex]) return;

                try {
                    await fillPlanDetails(page, zipcode, entry.plans[planIndex]);
                    
                    if (entry.plans[planIndex].status === PLAN_STATUS.COMPLETED) {
                        entry.plansWithDetails++;
                        state.metadata.totalPlansFilled++;
                    }

                    // Check if zipcode is complete
                    const allDone = entry.plans.every(p => p.status !== PLAN_STATUS.PENDING);
                    if (allDone) {
                        entry.status = ZIPCODE_STATUS.COMPLETED;
                        entry.phase2CompletedAt = new Date().toISOString();
                        state.metadata.phase2Completed++;
                    }

                    // SAVE IMMEDIATELY
                    await saveState();
                    await exportOutputs();

                    printProgress(2, zipcode, `✅ Plan ${planIndex + 1}: ${planId}`);

                } catch (err) {
                    entry.plans[planIndex].status = PLAN_STATUS.ERROR;
                    entry.plans[planIndex].error = err.message;
                    
                    await saveState();
                    await exportOutputs();
                }

                await sleep(500);
            },

            async failedRequestHandler({ request }) {
                const { zipcode, planIndex } = request.userData;
                const entry = state.zipcodes[zipcode];
                if (entry && entry.plans[planIndex]) {
                    entry.plans[planIndex].status = PLAN_STATUS.ERROR;
                    entry.plans[planIndex].error = 'Max retries exceeded';
                    await saveState();
                    await exportOutputs();
                }
            }
        });

        const phase2Requests = plansToFill.map((item, idx) => ({
            url: item.plan.detailsUrl || config.baseUrl,
            uniqueKey: `phase2-${item.zipcode}-${item.planIndex}-${idx}`,
            userData: {
                zipcode: item.zipcode,
                planIndex: item.planIndex,
                planId: item.plan.planId || `plan-${item.planIndex}`
            }
        }));

        await phase2Crawler.run(phase2Requests);
    }

    // ==========================================================================
    // FINAL SUMMARY
    // ==========================================================================
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);
    
    await saveState();
    const outputPaths = await exportOutputs();

    const final = getProgress();

    console.log('\n' + '═'.repeat(70));
    console.log('📊 FINAL SUMMARY');
    console.log('═'.repeat(70));
    console.log(`Duration: ${duration} minutes`);
    console.log(`\nZipcodes:`);
    console.log(`  Total: ${final.total}`);
    console.log(`  URLs Collected: ${final.phase1} (${final.phase1Pct}%)`);
    console.log(`  Fully Completed: ${final.phase2}`);
    console.log(`  Errors: ${final.errors}`);
    console.log(`\nPlans:`);
    console.log(`  Total Found: ${final.plansFound}`);
    console.log(`  Details Filled: ${final.plansFilled} (${final.phase2Pct}%)`);

    console.log('\n📁 Output Files:');
    console.log(`  1. ${outputPaths.jsonPath}`);
    console.log(`  2. ${outputPaths.summaryPath}`);
    console.log(`  3. ${outputPaths.detailsPath}`);
    console.log(`  4. ${config.outputDir}/${STATE_FILE} (for resume)`);

    if (errors.length > 0) {
        await exportErrors(errors, 'errors.json');
        console.log(`  5. ${config.outputDir}/errors.json`);
    }

    console.log('\n' + '═'.repeat(70));
    console.log(`Finished at: ${new Date().toISOString()}`);
    
    if (final.plansFilled < final.plansFound) {
        console.log(`\n💡 TIP: Run again to continue filling remaining ${final.plansFound - final.plansFilled} plans`);
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
