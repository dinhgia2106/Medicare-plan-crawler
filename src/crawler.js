/**
 * Core Crawler - Medicare Plan Wizard Navigation
 */

import { PlaywrightCrawler, Dataset } from 'crawlee';
import { config, sleep } from './config.js';
import { extractPlanList, extractPlanDetails, hasNextPage, goToNextPage } from './extractors.js';
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

        // Take screenshot of initial page for debugging
        console.log('Current URL:', page.url());

        // Step 1: Enter zipcode
        console.log('Step 1: Looking for zipcode input...');

        // Try multiple selectors for zipcode input
        const zipcodeSelectors = [
            '#zip-code',
            'input[id*="zip"]',
            'input[name*="zip"]',
            'input[placeholder*="ZIP"]',
            'input[aria-label*="ZIP"]',
            'input[type="text"]'
        ];

        let zipcodeInput = null;
        for (const selector of zipcodeSelectors) {
            try {
                zipcodeInput = await page.$(selector);
                if (zipcodeInput) {
                    console.log(`  Found zipcode input using: ${selector}`);
                    break;
                }
            } catch {
                continue;
            }
        }

        if (!zipcodeInput) {
            // Log page content for debugging
            const pageContent = await page.content();
            console.log('Page HTML snippet:', pageContent.substring(0, 2000));
            throw new Error('Could not find zipcode input field');
        }

        // Clear and enter zipcode
        await zipcodeInput.click({ clickCount: 3 });
        await zipcodeInput.fill(zipcode);
        console.log(`  Entered zipcode: ${zipcode}`);
        await sleep(config.delays.betweenActions);

        // Step 2: Select Medicare Advantage Plan (Part C)
        console.log('Step 2: Looking for Medicare Advantage option...');

        const planTypeSelectors = [
            'label:has-text("Medicare Advantage")',
            'text=Medicare Advantage',
            '[data-value*="MAPD"]',
            '[data-value*="MA"]',
            'input[type="radio"][id*="ma"]',
            'input[type="radio"][id*="advantage"]',
            '.plan-type-option:has-text("Medicare Advantage")',
            'button:has-text("Medicare Advantage")'
        ];

        let planTypeSelected = false;
        for (const selector of planTypeSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    await element.click();
                    console.log(`  Selected plan type using: ${selector}`);
                    planTypeSelected = true;
                    break;
                }
            } catch (e) {
                console.log(`  Selector ${selector} failed: ${e.message}`);
                continue;
            }
        }

        if (!planTypeSelected) {
            console.log('  Warning: Could not find Medicare Advantage selector, continuing anyway...');
        }

        await sleep(config.delays.betweenActions);

        // Step 3: Click Find Plans / Submit
        console.log('Step 3: Clicking Find Plans / Submit...');

        const findPlansSelectors = [
            'button:has-text("Find Plans")',
            'button:has-text("Search Plans")',
            'button:has-text("Get Started")',
            'button:has-text("Continue")',
            'button[type="submit"]',
            'a:has-text("Find Plans")',
            '[data-testid*="find"]',
            '[data-testid*="submit"]'
        ];

        let clicked = false;
        for (const selector of findPlansSelectors) {
            try {
                const button = await page.$(selector);
                if (button) {
                    const isVisible = await button.isVisible();
                    if (isVisible) {
                        await button.click();
                        console.log(`  Clicked using: ${selector}`);
                        clicked = true;
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

        if (!clicked) {
            throw new Error('Could not find Find Plans button');
        }

        // Wait for navigation - use domcontentloaded instead of networkidle
        console.log('  Waiting for page navigation...');
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });
        await sleep(config.delays.afterPageLoad);
        console.log('  Current URL after submit:', page.url());

        // Step 4: Handle "I don't get help" question (if present)
        console.log('Step 4: Looking for help status question...');

        const noHelpSelectors = [
            'label:has-text("I don\'t get help")',
            'label:has-text("don\'t get help")',
            'text=I don\'t get help',
            'input[value*="no_help"]',
            '[data-testid*="no-help"]'
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

        // Click Continue if present
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Step 5: Handle "Do you want to see drugs" question - Select No
        console.log('Step 5: Looking for drug coverage question...');

        const noDrugSelectors = [
            'label:has-text("No, I")',
            'text=No, I don\'t',
            'input[value="no"]',
            'input[type="radio"]:near(:text("No"))'
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

        // Click Next/Continue
        await clickContinueButton(page);
        await sleep(config.delays.afterPageLoad);

        // Step 6: Skip Adding Providers
        console.log('Step 6: Looking for skip providers option...');

        const skipSelectors = [
            'a:has-text("Skip")',
            'button:has-text("Skip")',
            'text=Skip this step',
            'a:has-text("Continue without")',
            'button:has-text("Continue without")'
        ];

        for (const selector of skipSelectors) {
            try {
                const element = await page.$(selector);
                if (element) {
                    const isVisible = await element.isVisible();
                    if (isVisible) {
                        await element.click();
                        console.log(`  Skipped using: ${selector}`);
                        break;
                    }
                }
            } catch {
                continue;
            }
        }

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
 * @param {import('playwright').Page} page - Playwright page object
 * @param {Object} zipcodeInfo - Zipcode information
 * @returns {Promise<Array>} Array of plan data
 */
async function extractAllPlans(page, zipcodeInfo) {
    const allPlans = [];
    let pageNum = 1;

    do {
        console.log(`\nExtracting plans from page ${pageNum}...`);

        // Get plan list from current page
        const planList = await extractPlanList(page);
        console.log(`Found ${planList.length} plans on page ${pageNum}`);

        // For each plan, get details
        for (let i = 0; i < planList.length; i++) {
            const plan = planList[i];
            console.log(`  Processing plan ${i + 1}/${planList.length}: ${plan.planName || 'Unknown'}`);

            try {
                // Click on plan to view details
                const planCards = await page.$$(config.selectors.planCards);
                if (planCards[i]) {
                    // Find the details link/button within the card
                    const detailsLink = await planCards[i].$('a:has-text("Details"), button:has-text("Details"), a:has-text("View")');

                    if (detailsLink) {
                        await detailsLink.click();
                        await page.waitForLoadState('networkidle', { timeout: config.timeouts.navigation });
                        await sleep(config.delays.afterPageLoad);

                        // Extract detailed information
                        const details = await extractPlanDetails(page);

                        // Combine summary and details
                        const fullPlanData = {
                            ...zipcodeInfo,
                            ...plan,
                            details,
                            scrapedAt: new Date().toISOString()
                        };

                        allPlans.push(fullPlanData);

                        // Save incrementally
                        await appendToJSON(fullPlanData);

                        // Go back to plan list
                        await page.goBack();
                        await page.waitForLoadState('networkidle', { timeout: config.timeouts.navigation });
                        await sleep(config.delays.betweenPlans);
                    } else {
                        // If no details link, just save summary
                        allPlans.push({
                            ...zipcodeInfo,
                            ...plan,
                            details: null,
                            scrapedAt: new Date().toISOString()
                        });
                    }
                }
            } catch (err) {
                console.error(`    Error processing plan: ${err.message}`);
                allPlans.push({
                    ...zipcodeInfo,
                    ...plan,
                    details: null,
                    error: err.message,
                    scrapedAt: new Date().toISOString()
                });
            }
        }

        // Check for next page
        if (await hasNextPage(page)) {
            console.log('Going to next page...');
            await goToNextPage(page);
            await sleep(config.delays.afterPageLoad);
            pageNum++;
        } else {
            break;
        }

    } while (true);

    return allPlans;
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
