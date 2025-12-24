/**
 * Data extraction functions for Medicare plan data
 * Updated based on actual HTML structure from medicare.gov
 */

import { config } from './config.js';

/**
 * Extract all plan cards from the listing page
 * Based on actual HTML structure with PlanCard components
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

        // Extract plans using page.evaluate for better performance
        const extractedPlans = await page.evaluate(() => {
            const planCards = document.querySelectorAll('.SearchResults__plan-card, li.e2e-plan-card');
            const results = [];

            planCards.forEach((card, index) => {
                try {
                    // Plan name from header
                    const nameEl = card.querySelector('h2[data-cy="plan-card-header"], .PlanCard__header');
                    const planName = nameEl ? nameEl.textContent.trim() : null;

                    // Get Plan ID from heading id attribute (e.g., "7808-heading" -> extract plan info)
                    const headingId = nameEl ? nameEl.id : '';

                    // Sub header contains carrier and plan ID
                    const subHeader = card.querySelector('.PlanCard__sub_header');
                    let carrier = null;
                    let planId = null;
                    if (subHeader) {
                        const spans = subHeader.querySelectorAll('span');
                        if (spans.length >= 1) carrier = spans[0].textContent.trim();
                        if (spans.length >= 3) {
                            const planIdText = spans[2].textContent.trim();
                            planId = planIdText.replace('Plan ID: ', '');
                        }
                    }

                    // Star rating
                    const starEl = card.querySelector('.StarRating__stars, .e2e-star-rating');
                    const starRating = starEl ? starEl.getAttribute('data-stars') : null;

                    // Monthly Premium
                    const premiumSection = card.querySelector('[data-testid="monthlyPremium"]');
                    const premiumEl = premiumSection ? premiumSection.querySelector('.mct-c-benefit') : null;
                    const monthlyPremium = premiumEl ? premiumEl.textContent.trim() : null;

                    // Total yearly cost
                    const yearlySection = card.querySelector('[data-testid="yearlyCost"]');
                    const yearlyEl = yearlySection ? yearlySection.querySelector('.mct-c-benefit') : null;
                    const yearlyCost = yearlyEl ? yearlyEl.textContent.trim() : null;

                    // Other costs (deductibles)
                    const otherCostsSection = card.querySelector('[data-testid="otherCosts"]');
                    let healthDeductible = null;
                    let drugDeductible = null;
                    let maxOutOfPocket = null;

                    if (otherCostsSection) {
                        const infoGroups = otherCostsSection.querySelectorAll('.PlanCard__info_group');
                        infoGroups.forEach(group => {
                            const label = group.querySelector('.Tooltip__trigger, button');
                            const value = group.querySelector('.mct-c-benefit');
                            if (label && value) {
                                const labelText = label.textContent.trim().toLowerCase();
                                if (labelText.includes('health deductible')) {
                                    healthDeductible = value.textContent.trim();
                                } else if (labelText.includes('drug deductible')) {
                                    drugDeductible = value.textContent.trim();
                                } else if (labelText.includes('maximum')) {
                                    maxOutOfPocket = value.textContent.trim();
                                }
                            }
                        });
                    }

                    // Benefits
                    const benefitsSection = card.querySelector('[data-testid="benefits"]');
                    const benefits = {};
                    if (benefitsSection) {
                        const benefitItems = benefitsSection.querySelectorAll('.PlanCard__benefits li');
                        benefitItems.forEach(item => {
                            const hasCheck = item.querySelector('[data-testid="checkmarkIcon"]');
                            const text = item.textContent.replace(/is (not )?available/g, '').trim();
                            benefits[text.toLowerCase()] = !!hasCheck;
                        });
                    }

                    // Copays
                    const copaysSection = card.querySelector('[data-testid="copays"]');
                    let primaryDoctorCopay = null;
                    let specialistCopay = null;
                    if (copaysSection) {
                        const copayGroups = copaysSection.querySelectorAll('.PlanCard__info_group');
                        copayGroups.forEach(group => {
                            const text = group.textContent;
                            if (text.includes('Primary doctor')) {
                                const copay = group.querySelector('.PlanCard__copay');
                                primaryDoctorCopay = copay ? copay.textContent.trim() : null;
                            }
                            if (text.includes('Specialist')) {
                                const copay = group.querySelector('.PlanCard__copay');
                                specialistCopay = copay ? copay.textContent.trim() : null;
                            }
                        });
                    }

                    // Plan details URL
                    const detailsLink = card.querySelector('.e2e-plan-details-btn, a[aria-label*="Plan details"]');
                    const detailsUrl = detailsLink ? detailsLink.href : null;

                    // Has drug coverage
                    const drugsSection = card.querySelector('[data-testid="drugs"]');
                    let hasDrugCoverage = true;
                    if (drugsSection) {
                        const noDrugIcon = drugsSection.querySelector('[data-testid="xIcon"]');
                        if (noDrugIcon) hasDrugCoverage = false;
                    }

                    results.push({
                        planName,
                        planId,
                        carrier,
                        starRating,
                        monthlyPremium,
                        yearlyCost,
                        healthDeductible,
                        drugDeductible,
                        maxOutOfPocket,
                        benefits,
                        primaryDoctorCopay,
                        specialistCopay,
                        hasDrugCoverage,
                        detailsUrl,
                        cardIndex: index
                    });
                } catch (err) {
                    console.error(`Error extracting card ${index}:`, err);
                }
            });

            return results;
        });

        plans.push(...extractedPlans);
        console.log(`Extracted ${plans.length} plans from page`);

    } catch (err) {
        console.error('Error extracting plan list:', err.message);
    }

    return plans;
}

/**
 * Extract detailed plan information from the plan details page
 * Based on actual HTML structure with sections: Overview, Benefits, Drug Coverage, etc.
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<Object>} Plan details object
 */
export async function extractPlanDetails(page) {
    try {
        // Wait for the details page to load - use faster selector wait
        await page.waitForSelector('.PlanDetailsPagePlanInfo, .e2e-plan-details-page', { timeout: 15000 });

        const details = await page.evaluate(() => {
            // Helper functions
            const getText = (selector, context = document) => {
                const elem = context.querySelector(selector);
                return elem ? elem.textContent.trim() : null;
            };

            const getTableData = (tableSelector) => {
                const table = document.querySelector(tableSelector);
                if (!table) return {};
                const rows = table.querySelectorAll('tbody tr');
                const data = {};
                rows.forEach(row => {
                    const header = row.querySelector('th');
                    const cell = row.querySelector('td');
                    if (header && cell) {
                        const key = header.textContent.trim().replace(/[?']/g, '').toLowerCase().replace(/\s+/g, '_');
                        data[key] = cell.textContent.trim();
                    }
                });
                return data;
            };

            // Basic Info
            const planName = getText('h1.e2e-plan-details-plan-header, .PlanDetailsPagePlanInfo h1');
            const planType = getText('.PlanDetailsPagePlanInfo .e2e-plan-details-plan-type');
            const planIdElement = document.querySelector('.PlanDetailsPagePlanInfo .PlanDetailsPagePlanInfo__value');
            const planId = planIdElement ? planIdElement.textContent.trim() : null;
            const carrier = getText('.PlanDetailsPagePlanInfo h2');

            // Contact Info
            const planContactSection = document.querySelector('.PlanDetailsPagePlanContact');
            let planWebsite = null;
            let nonMemberPhone = null;
            let memberPhone = null;
            if (planContactSection) {
                const websiteLink = planContactSection.querySelector('a[id="plan-contact"]');
                planWebsite = websiteLink ? websiteLink.href : null;
                nonMemberPhone = getText('#non-members-number', planContactSection);
                memberPhone = getText('#members-number', planContactSection);
            }

            // What you'll pay (summary)
            const whatYouPaySection = document.querySelector('.mct-c-what-youll-pay');
            let whatYouPay = {};
            if (whatYouPaySection) {
                const features = whatYouPaySection.querySelectorAll('.mct-c-what-youll-pay__feature');
                features.forEach(feature => {
                    const label = feature.querySelector('dt');
                    const value = feature.querySelector('dd .mct-c-benefit, dd .mct-c-what-youll-pay__cost');
                    if (label && value) {
                        const key = label.textContent.trim().toLowerCase().replace(/\s+/g, '_');
                        whatYouPay[key] = value.textContent.trim();
                    }
                });
            }

            // Overview Section - Premiums
            const premiums = {};
            const premiumsTable = document.querySelector('#overview table');
            if (premiumsTable) {
                const rows = premiumsTable.querySelectorAll('tbody tr');
                rows.forEach(row => {
                    const th = row.querySelector('th');
                    const td = row.querySelector('td');
                    if (th && td) {
                        const key = th.textContent.trim().split('\n')[0].toLowerCase().replace(/\s+/g, '_');
                        premiums[key] = td.textContent.trim();
                    }
                });
            }

            // Deductibles
            const deductibles = {};
            const deductiblesCaption = document.querySelector('caption:has(h3:contains("Deductibles"))');
            if (deductiblesCaption) {
                const table = deductiblesCaption.closest('table');
                if (table) {
                    const rows = table.querySelectorAll('tbody tr');
                    rows.forEach(row => {
                        const th = row.querySelector('th');
                        const td = row.querySelector('td');
                        if (th && td) {
                            const key = th.textContent.trim().toLowerCase().replace(/\s+/g, '_');
                            deductibles[key] = td.textContent.trim();
                        }
                    });
                }
            }

            // Benefits & Costs section
            const benefitsCosts = {};
            const benefitsSection = document.querySelector('#benefits');
            if (benefitsSection) {
                const tables = benefitsSection.querySelectorAll('table');
                tables.forEach(table => {
                    const caption = table.querySelector('caption h3');
                    if (caption) {
                        const sectionName = caption.textContent.trim().toLowerCase().replace(/\s+/g, '_');
                        benefitsCosts[sectionName] = [];
                        const rows = table.querySelectorAll('tbody tr');
                        rows.forEach(row => {
                            const th = row.querySelector('th');
                            const tds = row.querySelectorAll('td');
                            if (th && tds.length > 0) {
                                benefitsCosts[sectionName].push({
                                    service: th.textContent.trim(),
                                    cost: tds[0] ? tds[0].textContent.trim() : null,
                                    limits: tds[1] ? tds[1].textContent.trim() : null
                                });
                            }
                        });
                    }
                });
            }

            // Drug Coverage section
            const drugCoverage = {};
            const drugSection = document.querySelector('#drug-coverage');
            if (drugSection) {
                // Drug tiers
                const tiersTable = drugSection.querySelector('#CostsByDrugTierTable');
                if (tiersTable) {
                    drugCoverage.tiers = [];
                    const rows = tiersTable.querySelectorAll('tbody tr');
                    rows.forEach(row => {
                        const tierName = getText('th', row);
                        const initialCoverage = getText('[data-testid*="initial_coverage"]', row);
                        const catastrophic = getText('[data-testid*="catastrophic"]', row);
                        if (tierName) {
                            drugCoverage.tiers.push({
                                tier: tierName,
                                initialCoverage,
                                catastrophic
                            });
                        }
                    });
                }
            }

            // Extra Benefits section
            const extraBenefits = {};
            const extraSection = document.querySelector('#extra-benefits');
            if (extraSection) {
                const tables = extraSection.querySelectorAll('table');
                tables.forEach(table => {
                    const caption = table.querySelector('caption h3');
                    if (caption) {
                        const sectionName = caption.textContent.trim().toLowerCase().replace(/\s+/g, '_');
                        extraBenefits[sectionName] = [];
                        const rows = table.querySelectorAll('tbody tr');
                        rows.forEach(row => {
                            const th = row.querySelector('th');
                            const td = row.querySelector('td .mct-c-benefit');
                            if (th) {
                                extraBenefits[sectionName].push({
                                    benefit: th.textContent.trim(),
                                    coverage: td ? td.textContent.trim() : 'Not covered'
                                });
                            }
                        });
                    }
                });
            }

            // Star Ratings section
            let starRatings = {};
            const starSection = document.querySelector('#star-ratings');
            if (starSection) {
                const overallRating = starSection.querySelector('.e2e-star-rating');
                starRatings.overall = overallRating ? overallRating.getAttribute('data-stars') : null;
            }

            return {
                // Basic Info
                planName,
                planType,
                planId,
                carrier,

                // Contact
                planWebsite,
                nonMemberPhone,
                memberPhone,

                // Costs Summary
                whatYouPay,
                premiums,
                deductibles,

                // Benefits & Costs
                benefitsCosts,

                // Drug Coverage
                drugCoverage,

                // Extra Benefits
                extraBenefits,

                // Ratings
                starRatings,

                // Page URL
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
 * Get total number of plans from the results page
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<number>} Total number of plans
 */
export async function getTotalPlanCount(page) {
    try {
        const totalText = await page.textContent('#total-plan-results, #mct-sr-title');
        // Parse "Showing 10 of 58 Medicare Advantage Plans"
        const match = totalText.match(/of\s+(\d+)/);
        return match ? parseInt(match[1], 10) : 0;
    } catch {
        return 0;
    }
}

/**
 * Get comprehensive plan pagination info
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<Object>} Object with totalPlans, totalPages, plansPerPage
 */
export async function getTotalPlanInfo(page) {
    try {
        // Try to get the text from multiple possible selectors
        let totalText = null;
        const selectors = ['#total-plan-results', '#mct-sr-title', '.ds-u-visibility--screen-reader[aria-live="polite"]'];
        
        for (const selector of selectors) {
            try {
                totalText = await page.textContent(selector, { timeout: 5000 });
                if (totalText && totalText.includes('of')) break;
            } catch {
                continue;
            }
        }
        
        // Parse "Showing 10 of 58 Medicare Advantage Plans"
        if (totalText) {
            const showingMatch = totalText.match(/Showing\s+(\d+)\s+of\s+(\d+)/i);
            if (showingMatch) {
                const plansPerPage = parseInt(showingMatch[1], 10);
                const totalPlans = parseInt(showingMatch[2], 10);
                const totalPages = Math.ceil(totalPlans / plansPerPage);
                return { totalPlans, totalPages, plansPerPage };
            }
            
            // Try simpler pattern
            const simpleMatch = totalText.match(/of\s+(\d+)/i);
            if (simpleMatch) {
                const totalPlans = parseInt(simpleMatch[1], 10);
                return { totalPlans, totalPages: Math.ceil(totalPlans / 10), plansPerPage: 10 };
            }
        }
        
        // Fallback: count plans on page
        const planCards = await page.$$('.SearchResults__plan-card, li.e2e-plan-card');
        return { totalPlans: planCards.length, totalPages: 1, plansPerPage: planCards.length };
    } catch (err) {
        console.error('Error getting plan info:', err.message);
        return { totalPlans: 0, totalPages: 1, plansPerPage: 10 };
    }
}

/**
 * Check if there are more pages of results
 * @param {import('playwright').Page} page - Playwright page object
 * @returns {Promise<boolean>} True if there's a next page
 */
export async function hasNextPage(page) {
    try {
        // Look for pagination next button
        const nextButton = await page.$('.ds-c-pagination__item--next button, button[aria-label*="next page"], button:has-text("Next")');
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
        const nextButton = await page.$('.ds-c-pagination__item--next button, button[aria-label*="next page"], button:has-text("Next")');
        if (!nextButton) return false;

        await nextButton.click();
        await page.waitForLoadState('domcontentloaded', { timeout: config.timeouts.navigation });

        return true;
    } catch (err) {
        console.error('Error navigating to next page:', err.message);
        return false;
    }
}
