/**
 * Core Crawler - Medicare Plan Wizard Navigation
 */

import { PlaywrightCrawler, Dataset } from 'crawlee';
import { config, sleep } from './config.js';
import { extractPlanList, extractPlanDetails, hasNextPage, goToNextPage, goToPage, getTotalPlanInfo } from './extractors.js';
import { appendToJSON } from './exporters.js';

/**
 * Navigate through the Medicare wizard steps
 * @param {import('playwright').Page} page - Playwright page object
 * @param {string} zipcode - Zipcode to search for
 * @returns {Promise<boolean>} True if navigation was successful
 */
async function navigateWizard(page, zipcode) {
    console.log(`\n=== Navigating wizard for zipcode: ${zipcode} ===`);

    try {
        // Wait for page to stabilize
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(config.delays.afterPageLoad);

        console.log('Current URL:', page.url());

        // Step 1: Enter zipcode
        console.log('Step 1: Entering zipcode...');

        const zipcodeInput = await page.waitForSelector(
            'input[name*="zip"], input[id*="zip"], #zip-code',
            { timeout: config.timeouts.element }
        );

        await zipcodeInput.click({ clickCount: 3 });
        await zipcodeInput.fill(zipcode);
        console.log(`  Entered zipcode: ${zipcode}`);
        await sleep(config.delays.betweenActions);

        // Step 2: Click first Continue to go to plan type selection
        console.log('Step 2: Clicking Continue to see plan types...');
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Step 3: Select Medicare Advantage Plan (Part C)
        // Based on actual HTML: <input id="what-coverage-mapd" value="MEDICARE_ADVANTAGE_PLAN">
        console.log('Step 3: Selecting Medicare Advantage Plan (Part C)...');

        const planTypeSelectors = [
            '#what-coverage-mapd',  // Exact ID from HTML
            'input[value="MEDICARE_ADVANTAGE_PLAN"]',  // Exact value from HTML
            'label[for="what-coverage-mapd"]',  // The label
            '[data-testid="what-coverage-mapd"]',  // data-testid
            'input[name="coverage-selector-select-plan-type"][value="MEDICARE_ADVANTAGE_PLAN"]'
        ];

        let planTypeSelected = false;
        for (const selector of planTypeSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    const isVisible = await element.isVisible();
                    if (isVisible) {
                        await element.click();
                        console.log(`  Selected Medicare Advantage using: ${selector}`);
                        planTypeSelected = true;
                        break;
                    }
                }
            } catch (e) {
                continue;
            }
        }

        if (!planTypeSelected) {
            // Try waiting for the element
            try {
                await page.waitForSelector('#what-coverage-mapd', { timeout: 10000 });
                await page.click('#what-coverage-mapd');
                console.log('  Selected Medicare Advantage using waitForSelector');
                planTypeSelected = true;
            } catch {
                console.log('  Warning: Could not find Medicare Advantage selector');
            }
        }

        await sleep(config.delays.betweenActions);

        // Step 4: Click "Find Plans" button (NOT submit/continue which triggers login)
        console.log('Step 4: Clicking Find Plans button...');

        const findPlansSelectors = [
            'button:has-text("Find Plans")',
            'a:has-text("Find Plans")',
            '[data-testid*="find-plans"]',
            'button:has-text("Search Plans")',
            'button:has-text("View Plans")'
        ];

        let findPlansClicked = false;
        for (const selector of findPlansSelectors) {
            try {
                const button = await page.$(selector);
                if (button) {
                    const isVisible = await button.isVisible();
                    if (isVisible) {
                        await button.click();
                        console.log(`  Clicked Find Plans using: ${selector}`);
                        findPlansClicked = true;
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

        if (!findPlansClicked) {
            // Try waiting for Find Plans button
            try {
                await page.waitForSelector('button:has-text("Find Plans")', { timeout: 10000 });
                await page.click('button:has-text("Find Plans")');
                console.log('  Clicked Find Plans using waitForSelector');
                findPlansClicked = true;
            } catch {
                console.log('  Warning: Could not find Find Plans button');
            }
        }

        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(config.delays.afterPageLoad);
        console.log('  Current URL:', page.url());

        // Step 5: Handle "Do you get help paying for Medicare" question
        console.log('Step 5: Looking for help/subsidy question...');

        const noHelpSelectors = [
            'input[value="noSubsidy"]',
            'input[value="no"]',
            'label:has-text("I don\'t get help")',
            'label:has-text("No, I don\'t")',
            '#no-help',
            '[data-testid*="no-help"]',
            '[data-testid*="noSubsidy"]'
        ];

        for (const selector of noHelpSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    const isVisible = await element.isVisible();
                    if (isVisible) {
                        await element.click();
                        console.log(`  Selected no help using: ${selector}`);
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

        await sleep(config.delays.betweenActions);
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Step 6: Handle drug coverage question - Select No
        console.log('Step 6: Looking for drug coverage question...');

        const noDrugSelectors = [
            'input[value="no"]',
            'input[value="false"]',
            'label:has-text("No, I don\'t")',
            'label:has-text("No")',
            '#no-drugs',
            '[data-testid*="no-drugs"]'
        ];

        for (const selector of noDrugSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    const isVisible = await element.isVisible();
                    if (isVisible) {
                        await element.click();
                        console.log(`  Selected No using: ${selector}`);
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

        await sleep(config.delays.betweenActions);
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Step 7: Skip Adding Providers
        console.log('Step 7: Looking for skip providers option...');

        const skipSelectors = [
            '[data-testid="continue-to-plans"]',  // Exact selector from HTML
            'button:has-text("Skip Adding Providers")',
            'button:has-text("Skip")',
            'a:has-text("Skip")'
        ];

        for (const selector of skipSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    const isVisible = await element.isVisible();
                    if (isVisible) {
                        await element.click();
                        console.log(`  Skipped providers using: ${selector}`);
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

        // If no skip button, try clicking Continue
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Wait for plan results page
        console.log('Waiting for plan results...');
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(config.delays.afterPageLoad);

        console.log('Wizard navigation completed!');
        console.log('Final URL:', page.url());
        return true;

    } catch (err) {
        console.error(`Wizard navigation failed: ${err.message}`);
        // Take screenshot on error for debugging
        try {
            await page.screenshot({ path: './output/error_screenshot.png' });
            console.log('Error screenshot saved to ./output/error_screenshot.png');
        } catch { }
        return false;
    }
}

/**
 * Helper function to click Continue/Next button
 */
async function clickContinueButton(page) {
    const continueSelectors = [
        'button:has-text("Continue")',
        'button:has-text("Next")',
        'a:has-text("Continue")',
        'a:has-text("Next")',
        'button[type="submit"]'
    ];

    for (const selector of continueSelectors) {
        try {
            const button = await page.$(selector);
            if (button) {
                const isVisible = await button.isVisible();
                if (isVisible) {
                    await button.click();
                    console.log(`  Clicked continue/next: ${selector}`);
                    await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                    return true;
                }
            }
        } catch {
            continue;
        }
    }
    return false;
}

/**
 * Helper function to click a button by text
 */
async function clickButton(page, textOptions) {
    for (const text of textOptions) {
        try {
            const button = await page.$(`button:has-text("${text}"), a:has-text("${text}")`);
            if (button) {
                await button.click();
                console.log(`  Clicked button: ${text}`);
                return true;
            }
        } catch {
            continue;
        }
    }
    return false;
}

/**
 * Extract all plans for a given zipcode
 * Uses two-phase approach:
 * Phase 1: Collect all plan summaries and URLs by navigating through all pages
 * Phase 2: Visit each plan URL to extract detailed information
 * @param {import('playwright').Page} page - Playwright page object
 * @param {Object} zipcodeInfo - Zipcode information
 * @returns {Promise<Array>} Array of plan data
 */
async function extractAllPlans(page, zipcodeInfo) {
    const allPlans = [];
    const planSummaries = []; // Collect all plan summaries first
    const processedPlanIds = new Set();
    let pageNum = 1;

    // Get total plans count first
    const { totalPlans, totalPages, plansPerPage } = await getTotalPlanInfo(page);
    console.log('\n' + '='.repeat(60));
    console.log(`PLAN SUMMARY FOR ZIPCODE: ${zipcodeInfo.zipcode}`);
    console.log('='.repeat(60));
    console.log(`Total Plans: ${totalPlans} | Pages: ${totalPages} | Per Page: ${plansPerPage}`);
    console.log('='.repeat(60));

    // ============================================
    // PHASE 1: Collect all plan summaries and URLs
    // ============================================
    console.log('\n>>> PHASE 1: Collecting plan URLs from all pages...');

    for (let p = 1; p <= totalPages; p++) {
        console.log(`\n  [Page ${p}/${totalPages}]`);

        // Navigate to page if not the first one
        if (p > 1) {
            // Click pagination button for this page number
            const pageButtonClicked = await clickPaginationButton(page, p);
            if (!pageButtonClicked) {
                console.log(`  [WARN] Could not navigate to page ${p}, stopping collection`);
                break;
            }
            await page.waitForSelector(config.selectors.planCards, { timeout: 15000 }).catch(() => { });
            await sleep(1000);
        }

        // Extract plan list from current page
        const planList = await extractPlanList(page);
        console.log(`  Found ${planList.length} plans`);

        // Add unique plans to our collection
        let newOnThisPage = 0;
        for (const plan of planList) {
            const planId = plan.planId || `unknown-${p}-${planList.indexOf(plan)}`;
            if (!processedPlanIds.has(planId)) {
                processedPlanIds.add(planId);
                planSummaries.push(plan);
                newOnThisPage++;
            }
        }
        console.log(`  Added ${newOnThisPage} new plans (Total: ${planSummaries.length}/${totalPlans})`);

        // Stop if we have all plans
        if (planSummaries.length >= totalPlans) {
            console.log(`  Collected all ${totalPlans} plans`);
            break;
        }

        // Stop if no new plans found (loop detected)
        if (newOnThisPage === 0) {
            console.log(`  [WARN] No new plans on page ${p}, stopping collection`);
            break;
        }
    }

    console.log(`\n>>> Phase 1 complete: Collected ${planSummaries.length} unique plans\n`);

    // ============================================
    // PHASE 2: Visit each plan URL to get details
    // ============================================
    console.log('>>> PHASE 2: Extracting details for each plan...\n');

    let browserClosed = false;

    for (let i = 0; i < planSummaries.length; i++) {
        const plan = planSummaries[i];
        const planId = plan.planId || 'N/A';

        console.log(`  [${i + 1}/${planSummaries.length}] ${plan.planName || 'Unknown'} (${planId})`);

        // Check if browser/page is still alive before proceeding
        if (browserClosed || !(await isPageAlive(page))) {
            console.log(`     [SKIPPED] Browser/page closed - marking as failed`);
            allPlans.push({
                ...zipcodeInfo,
                ...plan,
                details: null,
                error: 'Browser context closed before extraction',
                scrapedAt: new Date().toISOString()
            });
            browserClosed = true;
            continue;
        }

        try {
            if (plan.detailsUrl) {
                console.log(`     -> Opening details...`);
                await page.goto(plan.detailsUrl, { waitUntil: 'domcontentloaded' });
                await page.waitForSelector('.PlanDetailsPagePlanInfo, .e2e-plan-details-page', { timeout: 15000 }).catch(() => { });
                await sleep(1000);

                // Extract detailed information
                const details = await extractPlanDetails(page);
                const extractedFields = Object.keys(details).filter(k => details[k] && k !== 'error' && k !== 'pageUrl');
                console.log(`     <- Extracted ${extractedFields.length} fields`);

                // Combine summary and details
                const fullPlanData = {
                    ...zipcodeInfo,
                    ...plan,
                    details,
                    scrapedAt: new Date().toISOString()
                };

                allPlans.push(fullPlanData);
                await appendToJSON(fullPlanData);
                console.log(`     [SAVED ${allPlans.length}/${planSummaries.length}]`);
            } else {
                console.log(`     [!] No details URL, saving summary only`);
                allPlans.push({
                    ...zipcodeInfo,
                    ...plan,
                    details: null,
                    scrapedAt: new Date().toISOString()
                });
            }
        } catch (err) {
            console.error(`     [ERROR] ${err.message}`);

            // Check if this is a browser closure error
            if (isBrowserClosedError(err)) {
                console.log(`     [!] Browser context closed - remaining plans will be marked as failed`);
                browserClosed = true;
            }

            allPlans.push({
                ...zipcodeInfo,
                ...plan,
                details: null,
                error: err.message,
                scrapedAt: new Date().toISOString()
            });
        }
    }

    // Final summary
    const successCount = allPlans.filter(p => !p.error).length;
    const failedCount = allPlans.filter(p => p.error).length;

    console.log('\n' + '='.repeat(60));
    console.log(`CRAWL COMPLETE: ${zipcodeInfo.zipcode}`);
    console.log(`Plans: ${allPlans.length} | Unique IDs: ${processedPlanIds.size}`);
    console.log(`Success: ${successCount}/${allPlans.length} | Failed: ${failedCount}`);
    if (browserClosed) {
        console.log(`[!] Browser closed during extraction - some plans may need re-crawling`);
    }
    console.log('='.repeat(60) + '\n');

    return allPlans;
}

/**
 * Click on a specific page number in pagination
 * @param {import('playwright').Page} page - Playwright page object
 * @param {number} targetPage - Page number to click
 * @returns {Promise<boolean>} True if successful
 */
async function clickPaginationButton(page, targetPage) {
    try {
        // Try to find and click pagination button with specific page number
        const selectors = [
            `button[aria-label*="Page ${targetPage}"]`,
            `a[aria-label*="Page ${targetPage}"]`,
            `.ds-c-pagination__item button:has-text("${targetPage}")`,
            `[data-testid="pagination-page-${targetPage}"]`,
            `.Pagination button:has-text("${targetPage}")`
        ];

        for (const selector of selectors) {
            try {
                const button = await page.$(selector);
                if (button) {
                    const isVisible = await button.isVisible();
                    if (isVisible) {
                        await button.click();
                        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
                        console.log(`  Clicked page ${targetPage} button`);
                        return true;
                    }
                }
            } catch {
                continue;
            }
        }

        // Fallback: try clicking "Next" button (targetPage - 1) times if we're on page 1
        // This is less reliable so we only use it as fallback
        const nextButton = await page.$('.ds-c-pagination__item--next button, button[aria-label*="next page"], button:has-text("Next")');
        if (nextButton) {
            await nextButton.click();
            await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
            console.log(`  Clicked Next (fallback for page ${targetPage})`);
            return true;
        }

        return false;
    } catch (err) {
        console.error(`  Error navigating to page ${targetPage}:`, err.message);
        return false;
    }
}

/**
 * Check if the page/browser context is still alive
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<boolean>} True if page is alive
 */
async function isPageAlive(page) {
    try {
        // Try a simple operation to check if page is still responsive
        await page.evaluate(() => true);
        return true;
    } catch (err) {
        // If this fails, the page/browser is closed
        return false;
    }
}

/**
 * Check if an error indicates browser/page closure
 * @param {Error} err - The error object
 * @returns {boolean} True if error indicates closure
 */
function isBrowserClosedError(err) {
    const closureMessages = [
        'Target page, context or browser has been closed',
        'Target closed',
        'Browser has been closed',
        'Context has been closed',
        'Connection closed',
        'Protocol error'
    ];
    return closureMessages.some(msg => err.message.includes(msg));
}

/**
 * Create and configure the PlaywrightCrawler
 * @param {Array} zipcodesData - Array of zipcode objects to crawl
 * @returns {PlaywrightCrawler} Configured crawler instance
 */
export function createCrawler(zipcodesData) {
    const allResults = [];
    const errors = [];

    const crawler = new PlaywrightCrawler({
        maxConcurrency: config.maxConcurrency,
        maxRequestRetries: config.maxRequestRetries,
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

            log.info(`Processing zipcode: ${zipcode} (${city}, ${state})`);

            try {
                // Navigate through the wizard
                const wizardSuccess = await navigateWizard(page, zipcode);

                if (!wizardSuccess) {
                    throw new Error('Failed to navigate wizard');
                }

                // Extract all plans
                const plans = await extractAllPlans(page, zipcodeInfo);

                log.info(`Extracted ${plans.length} plans for zipcode ${zipcode}`);

                // Store results
                await Dataset.pushData(plans);
                allResults.push(...plans);

            } catch (err) {
                log.error(`Error processing zipcode ${zipcode}: ${err.message}`);
                errors.push({
                    zipcode,
                    state,
                    city,
                    error: err.message,
                    timestamp: new Date().toISOString()
                });
            }

            // Delay between zipcodes
            await sleep(config.delays.betweenZipcodes);
        },

        async failedRequestHandler({ request, log }) {
            const { zipcode, state, city } = request.userData;
            log.error(`Request failed for zipcode ${zipcode} after retries`);

            errors.push({
                zipcode,
                state,
                city,
                error: 'Max retries exceeded',
                timestamp: new Date().toISOString()
            });
        }
    });

    // Attach results and errors for access after crawling
    crawler.allResults = allResults;
    crawler.errors = errors;

    return crawler;
}
