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

import { readFile, writeFile, mkdir, rm, rename, copyFile } from 'fs/promises';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import { PlaywrightCrawler } from 'crawlee';
import { config, sleep } from './config.js';
import { extractPlanList, extractPlanDetails, getTotalPlanInfo } from './extractors.js';
import { exportErrors } from './exporters.js';
import { existsSync } from 'fs';
import * as readline from 'readline';

const DEFAULT_WORKERS = 10;  // Increased for high-performance systems (64GB RAM, RTX 5070 Ti, Ryzen 7500F)
const STATE_FILE = 'crawler_state.json';
const OUTPUT_JSON = 'medicare_plans.json';
const OUTPUT_CSV_SUMMARY = 'medicare_plans_summary.csv';
const OUTPUT_CSV_PLANS = 'medicare_plans_details.csv';
const CRAWLEE_STORAGE = './storage';

// ============================================================================
// SAFE FILE WRITING & GRACEFUL SHUTDOWN
// ============================================================================

let isShuttingDown = false;
let isSaving = false;
let saveQueue = [];
let saveTimer = null;
const SAVE_DEBOUNCE_MS = 1000;  // Batch saves every 1 second (faster with SSD)

/**
 * Mutex lock for file writing
 */
class WriteLock {
    constructor() {
        this.locked = false;
        this.queue = [];
    }

    async acquire() {
        return new Promise((resolve) => {
            if (!this.locked) {
                this.locked = true;
                resolve();
            } else {
                this.queue.push(resolve);
            }
        });
    }

    release() {
        if (this.queue.length > 0) {
            const next = this.queue.shift();
            next();
        } else {
            this.locked = false;
        }
    }
}

const writeLock = new WriteLock();

/**
 * Atomic write - write to temp file, then rename
 * This prevents corruption if interrupted mid-write
 */
async function atomicWriteFile(filePath, content) {
    const tempPath = `${filePath}.tmp`;
    const backupPath = `${filePath}.backup`;

    try {
        // Write to temp file first
        await writeFile(tempPath, content, 'utf-8');

        // If original exists, create backup
        if (existsSync(filePath)) {
            await copyFile(filePath, backupPath);
        }

        // Atomic rename (this is atomic on most filesystems)
        await rename(tempPath, filePath);

    } catch (err) {
        // If rename failed, try to restore from backup
        if (existsSync(backupPath) && !existsSync(filePath)) {
            await copyFile(backupPath, filePath);
        }
        throw err;
    }
}

/**
 * Debounced save - batches multiple save requests
 */
function scheduleSave() {
    if (saveTimer) return;  // Already scheduled

    saveTimer = setTimeout(async () => {
        saveTimer = null;
        await doSave();
    }, SAVE_DEBOUNCE_MS);
}

/**
 * Immediate save with lock - ensures only one write at a time
 */
async function doSave() {
    if (isShuttingDown) return;

    await writeLock.acquire();
    try {
        isSaving = true;

        const statePath = `${config.outputDir}/${STATE_FILE}`;
        state.metadata.lastUpdatedAt = new Date().toISOString();
        await atomicWriteFile(statePath, JSON.stringify(state, null, 2));

        // Export outputs
        await doExportOutputs();

    } catch (err) {
        console.error(`❌ Save error: ${err.message}`);
    } finally {
        isSaving = false;
        writeLock.release();
    }
}

/**
 * Graceful shutdown handler
 */
async function gracefulShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log(`\n\n⚠️  Received ${signal}, saving state before exit...`);

    // Cancel any pending debounced save
    if (saveTimer) {
        clearTimeout(saveTimer);
        saveTimer = null;
    }

    try {
        // Wait for any ongoing save to complete
        let waitCount = 0;
        while (isSaving && waitCount < 100) {
            await sleep(100);
            waitCount++;
        }

        // Final save with lock
        await doSave();

        const p = getProgress();
        console.log(`\n✅ State saved successfully!`);
        console.log(`   Phase 1: ${p.phase1}/${p.total} zipcodes`);
        console.log(`   Phase 2: ${p.plansFilled}/${p.plansFound} plans`);
        console.log(`\n💡 Run again to continue from where you left off.`);

    } catch (err) {
        console.error(`❌ Error saving state: ${err.message}`);
        console.log(`   Check ${config.outputDir}/${STATE_FILE}.backup if needed`);
    }

    process.exit(0);
}

// Register shutdown handlers
process.on('SIGINT', () => gracefulShutdown('SIGINT'));   // Ctrl+C
process.on('SIGTERM', () => gracefulShutdown('SIGTERM')); // kill command

/**
 * Ask user yes/no question
 */
async function askUser(question) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise((resolve) => {
        rl.question(question, (answer) => {
            rl.close();
            resolve(answer.toLowerCase().startsWith('y'));
        });
    });
}

// ============================================================================
// TIME TRACKING FOR ETA
// ============================================================================

const timeTracker = {
    phase1StartTime: null,
    phase2StartTime: null,
    phase1Times: [],  // Array of ms per zipcode
    phase2Times: [],  // Array of ms per plan
    lastItemTime: null
};

/**
 * Calculate ETA based on average processing time
 */
function calculateETA(phase, remaining) {
    const times = phase === 1 ? timeTracker.phase1Times : timeTracker.phase2Times;

    if (times.length < 3) return 'calculating...';

    // Use last 20 items for more accurate recent average
    const recentTimes = times.slice(-20);
    const avgMs = recentTimes.reduce((a, b) => a + b, 0) / recentTimes.length;

    const remainingMs = avgMs * remaining;
    return formatDuration(remainingMs);
}

/**
 * Format milliseconds to human readable duration
 */
function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
        return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    } else {
        return `${seconds}s`;
    }
}

/**
 * Record time for an item
 */
function recordTime(phase) {
    const now = Date.now();
    if (timeTracker.lastItemTime) {
        const elapsed = now - timeTracker.lastItemTime;
        if (phase === 1) {
            timeTracker.phase1Times.push(elapsed);
        } else {
            timeTracker.phase2Times.push(elapsed);
        }
    }
    timeTracker.lastItemTime = now;
}

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
 * Load state from disk if exists (with backup fallback)
 */
async function loadState() {
    const statePath = `${config.outputDir}/${STATE_FILE}`;
    const backupPath = `${statePath}.backup`;
    const jsonPath = `${config.outputDir}/${OUTPUT_JSON}`;

    // Try main state file first
    if (existsSync(statePath)) {
        try {
            const content = await readFile(statePath, 'utf-8');
            const loaded = JSON.parse(content);
            state = loaded;
            console.log(`📂 Loaded existing state from ${statePath}`);
            console.log(`   - Phase 1 (URLs): ${state.metadata.phase1Completed}/${state.metadata.totalZipcodes} zipcodes`);
            console.log(`   - Phase 2 (Details): ${state.metadata.totalPlansFilled}/${state.metadata.totalPlansFound} plans`);
            return { success: true };
        } catch (err) {
            console.warn(`⚠️ Main state file corrupted: ${err.message}`);
        }
    }

    // Try backup state file
    if (existsSync(backupPath)) {
        try {
            console.log(`   Trying backup file...`);
            const content = await readFile(backupPath, 'utf-8');
            const loaded = JSON.parse(content);
            state = loaded;
            console.log(`📂 Loaded state from BACKUP: ${backupPath}`);
            console.log(`   - Phase 1 (URLs): ${state.metadata.phase1Completed}/${state.metadata.totalZipcodes} zipcodes`);
            console.log(`   - Phase 2 (Details): ${state.metadata.totalPlansFilled}/${state.metadata.totalPlansFound} plans`);
            return { success: true, fromBackup: true };
        } catch (err) {
            console.warn(`⚠️ Backup state also corrupted: ${err.message}`);
        }
    }

    // Try to recover from medicare_plans.json
    if (existsSync(jsonPath)) {
        try {
            console.log(`   Trying to recover from ${OUTPUT_JSON}...`);
            const content = await readFile(jsonPath, 'utf-8');
            const plansData = JSON.parse(content);

            // Rebuild state from JSON
            let phase1Done = 0, phase2Done = 0, totalPlans = 0, plansFilled = 0;

            for (const entry of plansData) {
                if (entry.status === 'urls_collected' || entry.status === 'completed') {
                    phase1Done++;
                    totalPlans += entry.totalPlans || 0;
                }
                if (entry.status === 'completed') {
                    phase2Done++;
                    plansFilled += entry.plansWithDetails || 0;
                } else {
                    for (const plan of entry.plans || []) {
                        if (plan.status === 'completed') plansFilled++;
                    }
                }
            }

            state = {
                metadata: {
                    createdAt: new Date().toISOString(),
                    lastUpdatedAt: new Date().toISOString(),
                    totalZipcodes: plansData.length,
                    phase1Completed: phase1Done,
                    phase2Completed: phase2Done,
                    totalPlansFound: totalPlans,
                    totalPlansFilled: plansFilled,
                    workers: DEFAULT_WORKERS
                },
                zipcodeOrder: plansData.map(e => e.zipcode),
                zipcodes: {}
            };

            for (const entry of plansData) {
                state.zipcodes[entry.zipcode] = {
                    index: entry.index,
                    zipcode: entry.zipcode,
                    state: entry.state,
                    city: entry.city,
                    status: entry.status,
                    phase1StartedAt: null,
                    phase1CompletedAt: null,
                    phase2StartedAt: null,
                    phase2CompletedAt: null,
                    totalPlans: entry.totalPlans || 0,
                    plansWithDetails: entry.plansWithDetails || 0,
                    error: entry.error,
                    plans: entry.plans || []
                };
            }

            console.log(`✅ Recovered state from ${OUTPUT_JSON}`);
            console.log(`   - Phase 1 (URLs): ${phase1Done}/${plansData.length} zipcodes`);
            console.log(`   - Phase 2 (Details): ${plansFilled}/${totalPlans} plans`);
            return { success: true, recovered: true };

        } catch (err) {
            console.warn(`⚠️ Could not recover from JSON: ${err.message}`);
        }
    }

    // Nothing found
    return { success: false };
}

/**
 * Save state - uses debounced save to batch multiple requests
 * This prevents race conditions when multiple workers try to save simultaneously
 */
async function saveState() {
    scheduleSave();
}

/**
 * Force immediate save (used at critical points)
 */
async function saveStateImmediate() {
    if (saveTimer) {
        clearTimeout(saveTimer);
        saveTimer = null;
    }
    await doSave();
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
 * Print progress with ETA
 */
function printProgress(phase, zipcode, action) {
    const p = getProgress();

    if (phase === 1) {
        const remaining = p.total - p.phase1;
        const eta = calculateETA(1, remaining);
        console.log(`\n📍 Phase 1 ${progressBar(p.phase1, p.total)} ${p.phase1Pct}% | ${p.phase1}/${p.total} URLs | 📊 ${p.plansFound} plans | ⏱️ ETA: ${eta}`);
    } else {
        const remaining = p.plansFound - p.plansFilled;
        const eta = calculateETA(2, remaining);
        console.log(`\n📋 Phase 2 ${progressBar(p.plansFilled, p.plansFound)} ${p.phase2Pct}% | ${p.plansFilled}/${p.plansFound} details | ⏱️ ETA: ${eta}`);
    }

    if (zipcode && action) {
        console.log(`   ${action}: ${zipcode}`);
    }
}

/**
 * Export outputs - just schedules a save (actual export done in doExportOutputs)
 */
async function exportOutputs() {
    scheduleSave();
}

/**
 * Actually export outputs (JSON + CSVs) with atomic writes
 * Called by doSave() with lock held
 */
async function doExportOutputs() {
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
    await atomicWriteFile(jsonPath, JSON.stringify(jsonData, null, 2));

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
    await atomicWriteFile(summaryPath, stringify(summaryData, { header: true }));

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
    await atomicWriteFile(detailsPath, stringify(detailsData, { header: true }));

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
    afterPageLoad: 1000,   // Reduced from 2000ms
    betweenActions: 500,   // Reduced from 1000ms
    afterClick: 800        // Reduced from 1500ms
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
        await page.waitForSelector('input[type="checkbox"], input[type="radio"]', { timeout: 10000 }).catch(() => { });
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
        } catch { }

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
            await page.waitForSelector(config.selectors.planCards, { timeout: 15000 }).catch(() => { });
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
        await page.waitForSelector('.PlanDetailsPagePlanInfo, .e2e-plan-details-page', { timeout: 15000 }).catch(() => { });
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

    // Auto-clear Crawlee storage to allow fresh run with our state
    console.log(`\n🧹 Clearing Crawlee storage for clean resume...`);
    try {
        await rm(`${CRAWLEE_STORAGE}/request_queues`, { recursive: true, force: true });
        await rm(`${CRAWLEE_STORAGE}/key_value_stores`, { recursive: true, force: true });
    } catch (e) {
        // Ignore if doesn't exist
    }

    // Load zipcodes from CSV
    let zipcodes = await loadZipcodes();
    if (options.limit) {
        zipcodes = zipcodes.slice(0, options.limit);
        console.log(`   Limited to first ${options.limit} zipcodes`);
    }

    // Load or initialize state
    const loadResult = await loadState();

    if (options.reset) {
        // User explicitly wants fresh start
        initializeState(zipcodes, options.workers);
        await saveStateImmediate();
        console.log(`\n📁 Fresh start (--reset) - all outputs initialized`);
    } else if (!loadResult.success) {
        // Could not load any state - check if output files exist
        const hasExistingData = existsSync(`${config.outputDir}/${OUTPUT_JSON}`) ||
            existsSync(`${config.outputDir}/${STATE_FILE}`);

        if (hasExistingData) {
            console.log(`\n❌ ERROR: Could not load state but existing data files found!`);
            console.log(`   This might mean data is corrupted.`);
            console.log(`\n   Options:`);
            console.log(`   1. Check ${config.outputDir}/${STATE_FILE}.backup`);
            console.log(`   2. Check ${config.outputDir}/${OUTPUT_JSON}.backup`);
            console.log(`   3. Run with --reset to start fresh (WILL LOSE ALL DATA)`);
            console.log(`\n   To recover manually, you can try:`);
            console.log(`   python3 -c "import json; d=json.load(open('${config.outputDir}/${OUTPUT_JSON}')); print(len(d), 'zipcodes')"`);
            process.exit(1);
        }

        // No existing data, safe to start fresh
        initializeState(zipcodes, options.workers);
        await saveStateImmediate();
        console.log(`\n📁 Fresh start - all outputs initialized`);
    } else {
        // Successfully loaded state
        mergeState(zipcodes, options.workers);
        await saveStateImmediate();
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
        timeTracker.phase1StartTime = Date.now();
        timeTracker.lastItemTime = Date.now();

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

                    recordTime(1);  // Track time for ETA
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

                    recordTime(1);  // Track time for ETA even on errors
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
        timeTracker.phase2StartTime = Date.now();
        timeTracker.lastItemTime = Date.now();

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

                    recordTime(2);  // Track time for ETA
                    printProgress(2, zipcode, `✅ Plan ${planIndex + 1}: ${planId}`);

                } catch (err) {
                    entry.plans[planIndex].status = PLAN_STATUS.ERROR;
                    entry.plans[planIndex].error = err.message;

                    await saveState();
                    await exportOutputs();

                    recordTime(2);  // Track time for ETA even on errors
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
