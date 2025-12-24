/**
 * Data extraction functions for Medicare plan data
 */

import { config } from './config.js';

/**
 * Extract all plan cards from the listing page
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<Array>} Array of plan summary objects
 */
export async function extractPlanList(page) {
    const plans = [];

    try {
        // Wait for plan cards to load
        await page.waitForSelector(config.selectors.planCards, {
            timeout: config.timeouts.planLoad
        });

        // Get all plan cards
        const planCards = await page.$$(config.selectors.planCards);
        console.log(`Found ${planCards.length} plan cards on current page`);

        for (let i = 0; i < planCards.length; i++) {
            const card = planCards[i];

            try {
                const planSummary = await card.evaluate((el) => {
                    // Extract basic info from the card
                    const getText = (selector) => {
                        const elem = el.querySelector(selector);
                        return elem ? elem.textContent.trim() : null;
                    };

                    const getAttr = (selector, attr) => {
                        const elem = el.querySelector(selector);
                        return elem ? elem.getAttribute(attr) : null;
                    };

                    return {
                        planName: getText('h2, h3, [class*="name"], [data-testid*="name"]'),
                        planId: getText('[class*="id"], [data-testid*="id"]'),
                        monthlyPremium: getText('[class*="premium"], [data-testid*="premium"]'),
                        annualDeductible: getText('[class*="deductible"], [data-testid*="deductible"]'),
                        starRating: getText('[class*="star"], [class*="rating"]'),
                        planType: getText('[class*="type"]'),
                        detailsUrl: getAttr('a[href*="detail"], a[href*="plan"]', 'href')
                    };
                });

                plans.push({
                    ...planSummary,
                    cardIndex: i
                });
            } catch (err) {
                console.error(`Error extracting plan card ${i}:`, err.message);
            }
        }
    } catch (err) {
        console.error('Error extracting plan list:', err.message);
    }

    return plans;
}

/**
 * Extract detailed plan information from the plan details page
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<Object>} Plan details object
 */
export async function extractPlanDetails(page) {
    try {
        // Wait for the details page to load
        await page.waitForLoadState('networkidle', { timeout: config.timeouts.navigation });

        const details = await page.evaluate(() => {
            const getText = (selector) => {
                const elem = document.querySelector(selector);
                return elem ? elem.textContent.trim() : null;
            };

            const getAllText = (selector) => {
                const elems = document.querySelectorAll(selector);
                return Array.from(elems).map(el => el.textContent.trim());
            };

            // Extract comprehensive plan details
            return {
                // Basic Info
                planName: getText('h1, [class*="plan-name"], [data-testid*="plan-name"]'),
                planId: getText('[class*="plan-id"], [data-testid*="plan-id"]'),
                contractId: getText('[class*="contract"], [data-testid*="contract"]'),

                // Costs
                monthlyPremium: getText('[data-testid*="premium"], [class*="premium"]'),
                annualDeductible: getText('[data-testid*="deductible"], [class*="deductible"]'),
                maxOutOfPocket: getText('[data-testid*="out-of-pocket"], [class*="max-oop"]'),

                // Ratings
                overallStarRating: getText('[class*="overall-rating"], [data-testid*="star-rating"]'),
                healthPlanRating: getText('[class*="health-rating"]'),
                drugPlanRating: getText('[class*="drug-rating"]'),

                // Coverage
                drugCoverage: getText('[class*="drug-coverage"], [data-testid*="drug"]'),
                dentalCoverage: getText('[class*="dental"], [data-testid*="dental"]'),
                visionCoverage: getText('[class*="vision"], [data-testid*="vision"]'),
                hearingCoverage: getText('[class*="hearing"], [data-testid*="hearing"]'),
                fitnessBenefit: getText('[class*="fitness"], [data-testid*="fitness"]'),

                // Provider Network
                networkType: getText('[class*="network-type"], [data-testid*="network"]'),
                providerDirectory: getText('[class*="provider-directory"]'),

                // Additional Benefits
                additionalBenefits: getAllText('[class*="benefit-item"], [data-testid*="benefit"]'),

                // Plan Contact
                planPhone: getText('[class*="phone"], [data-testid*="phone"]'),
                planWebsite: getText('[class*="website"], [data-testid*="website"]'),

                // Carrier Info
                carrierName: getText('[class*="carrier"], [class*="company"], [data-testid*="carrier"]'),

                // Get the full page URL
                pageUrl: window.location.href
            };
        });

        return details;
    } catch (err) {
        console.error('Error extracting plan details:', err.message);
        return { error: err.message };
    }
}

/**
 * Check if there are more pages of results
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<boolean>} True if there's a next page
 */
export async function hasNextPage(page) {
    try {
        const nextButton = await page.$(config.selectors.nextPageButton);
        if (!nextButton) return false;

        const isDisabled = await nextButton.evaluate(el =>
            el.disabled || el.classList.contains('disabled') || el.getAttribute('aria-disabled') === 'true'
        );

        return !isDisabled;
    } catch {
        return false;
    }
}

/**
 * Navigate to the next page of results
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<boolean>} True if navigation was successful
 */
export async function goToNextPage(page) {
    try {
        const nextButton = await page.$(config.selectors.nextPageButton);
        if (!nextButton) return false;

        await nextButton.click();
        await page.waitForLoadState('networkidle', { timeout: config.timeouts.navigation });

        return true;
    } catch (err) {
        console.error('Error navigating to next page:', err.message);
        return false;
    }
}
