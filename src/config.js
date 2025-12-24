/**
 * Configuration settings for the Medicare Plan Crawler
 */

export const config = {
    // Base URL for Medicare Plan Compare
    baseUrl: 'https://www.medicare.gov/plan-compare/#/?year=2026&lang=en',

    // Input/Output paths
    inputFile: './ZipCodes.csv',
    outputDir: './output',

    // Crawler settings
    maxConcurrency: 1, // Process one zipcode at a time to avoid rate limiting
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 300, // 5 minutes per request

    // Browser settings
    headless: false, // Set to false for debugging (visible browser)
    slowMo: 150, // Slow down actions by 150ms for better visibility

    // Delay settings (in milliseconds)
    delays: {
        betweenActions: { min: 1000, max: 2000 },
        afterPageLoad: { min: 2000, max: 3000 },
        betweenPlans: { min: 500, max: 1000 },
        betweenZipcodes: { min: 3000, max: 5000 }
    },

    // Timeouts
    timeouts: {
        navigation: 90000,  // 90 seconds for slow page loads
        element: 30000,
        planLoad: 45000
    },

    // Selectors (these may need adjustment based on actual site structure)
    selectors: {
        // Initial page
        zipcodeInput: 'input[id*="zipcode"], input[name*="zipcode"], input[placeholder*="ZIP"]',
        planTypeDropdown: 'select[id*="planType"], [data-testid*="plan-type"]',
        planTypeOption: 'Medicare Advantage Plan',
        findPlansButton: 'button:has-text("Find Plans"), [data-testid*="find-plans"]',

        // Help question page
        noHelpOption: 'input[type="radio"][value*="no"], label:has-text("I don\'t get help")',
        continueButton: 'button:has-text("Continue")',

        // Drug coverage question
        noDrugsOption: 'input[type="radio"][value="no"], label:has-text("No")',
        nextButton: 'button:has-text("Next")',

        // Providers page
        skipProvidersButton: 'button:has-text("Skip"), a:has-text("Skip")',

        // Plan listing
        planCards: '[data-testid*="plan-card"], .plan-card, [class*="PlanCard"]',
        planName: '[data-testid*="plan-name"], .plan-name, h2, h3',
        planDetailsLink: 'a:has-text("Plan Details"), button:has-text("View Details")',

        // Pagination
        nextPageButton: 'button:has-text("Next"), [aria-label*="next page"]',
        paginationInfo: '[class*="pagination"], [data-testid*="pagination"]'
    }
};

/**
 * Generate a random delay within the specified range
 */
export function randomDelay(delayConfig) {
    const { min, max } = delayConfig;
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Sleep for a random duration
 */
export async function sleep(delayConfig) {
    const ms = randomDelay(delayConfig);
    return new Promise(resolve => setTimeout(resolve, ms));
}
