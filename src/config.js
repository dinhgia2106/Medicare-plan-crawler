/**
 * Configuration settings for the Medicare Plan Crawler
 * Updated based on actual HTML structure from medicare.gov
 */

export const config = {
    // Base URL for Medicare Plan Compare
    baseUrl: 'https://www.medicare.gov/plan-compare/#/?year=2026&lang=en',

    // Direct search URL template (skip wizard by going directly to search results)
    searchUrlTemplate: 'https://www.medicare.gov/plan-compare/#/search-results?plan_type=PLAN_TYPE_MAPD&zip={zipcode}&fips={fips}&year=2026&lang=en',

    // Plan details URL template
    planDetailsUrlTemplate: 'https://www.medicare.gov/plan-compare/#/plan-details/{planId}?plan_type=PLAN_TYPE_MAPD&zip={zipcode}&fips={fips}&year=2026&lang=en',

    // Input/Output paths
    inputFile: './ZipCodes.csv',
    outputDir: './output',

    // Crawler settings
    maxConcurrency: 1, // Process one zipcode at a time to avoid rate limiting
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 600, // 10 minutes per request (for large zipcodes)

    // Browser settings
    headless: true, // Set to false for debugging (visible browser)
    slowMo: 50, // Reduced from 100ms for faster performance on powerful hardware

    delays: {
        betweenActions: { min: 300, max: 600 },
        afterPageLoad: { min: 1000, max: 1500 },
        betweenPlans: { min: 500, max: 1000 },
        betweenZipcodes: { min: 1500, max: 2500 }
    },

    // Timeouts
    timeouts: {
        navigation: 90000,  // 90 seconds for slow page loads
        element: 30000,
        planLoad: 60000
    },

    // Selectors based on actual HTML structure
    selectors: {
        // Plan listing page
        planCardsList: '.e2e-search-results-list, [data-testid="search-results-list"], ol.SearchResults__results-list',
        planCards: '.SearchResults__plan-card, [data-cy="e2e-plan-card-plan-type-mapd"], [data-cy="e2e-plan-card-plan-type-ma"], li.e2e-plan-card',

        // Plan card elements
        planName: '.PlanCard__header, h2[data-cy="plan-card-header"]',
        planId: '.PlanCard__sub_header span:nth-child(3)',
        carrier: '.PlanCard__sub_header span:first-child',
        starRating: '.StarRating__stars, .e2e-star-rating',

        // Cost elements in plan card
        monthlyPremium: '[data-testid="monthlyPremium"] .mct-c-benefit',
        yearlyCost: '[data-testid="yearlyCost"] .mct-c-benefit',
        healthDeductible: '[data-testid="otherCosts"] .mct-c-benefit',

        // Benefits
        benefitsList: '.PlanCard__benefits li',

        // Copays
        primaryDoctorCopay: '[data-testid="copays"] .PlanCard__info_group:first-child .PlanCard__copay',
        specialistCopay: '[data-testid="copays"] .PlanCard__info_group:nth-child(2) .PlanCard__copay',

        // Plan details link
        planDetailsLink: '.e2e-plan-details-btn, a[aria-label*="Plan details"]',

        // Pagination
        totalResults: '#total-plan-results',
        nextPageButton: 'button:has-text("Next"), [aria-label*="next page"], .ds-c-pagination__next',
        paginationInfo: '.Pagination, .ds-c-pagination',

        // Plan Details Page
        detailsPageHeader: '.PlanDetailsPagePlanInfo h1, .e2e-plan-details-plan-header',
        premiumsTable: 'table:has(caption:has-text("Premiums"))',
        deductiblesTable: 'table:has(caption:has-text("Deductibles"))',
        benefitsTable: '#benefits table',
        drugCoverageTable: '#drug-coverage table',
        extraBenefitsSection: '#extra-benefits',
        starRatingsSection: '#star-ratings'
    },

    // Plan types
    planTypes: {
        MAPD: 'PLAN_TYPE_MAPD',  // Medicare Advantage with drug coverage
        MA: 'PLAN_TYPE_MA',      // Medicare Advantage without drug
        PDP: 'PLAN_TYPE_PDP'     // Prescription Drug Plan
    }
};

/**
 * Generate a random delay within the specified range
 * @param {Object} delayConfig - Object with min and max properties
 */
export function randomDelay(delayConfig) {
    const { min, max } = delayConfig;
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Sleep for a specified duration
 * @param {number|Object} delayConfig - Either a number (ms) or an object with min/max properties
 */
export async function sleep(delayConfig) {
    let ms;
    if (typeof delayConfig === 'number') {
        ms = delayConfig;
    } else {
        ms = randomDelay(delayConfig);
    }
    return new Promise(resolve => setTimeout(resolve, ms));
}
