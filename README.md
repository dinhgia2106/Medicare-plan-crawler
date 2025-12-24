# Medicare Plan Crawler

A web crawler built with Crawlee and Playwright to scrape Medicare Advantage Plan data from medicare.gov.

## Features

- Crawls Medicare plan comparison for multiple zipcodes
- Navigates through the multi-step wizard automatically
- Extracts plan listings and detailed plan information
- Exports data to both JSON and CSV formats
- Includes retry logic and error handling
- Saves progress incrementally

## Prerequisites

- Node.js >= 18.0.0
- npm

## Installation

```bash
npm install
npx playwright install chromium
```

## Usage

### Test with a single zipcode

```bash
npm run test
```

Or specify a particular zipcode:

```bash
node src/main.js --test --zipcode 60601
```

### Run full crawler

```bash
npm start
```

### Limit number of zipcodes

```bash
node src/main.js --limit 10
```

## Configuration

Edit `src/config.js` to customize:

- **Delays**: Adjust timing between actions
- **Headless mode**: Set `headless: false` to see the browser
- **Retry settings**: Configure max retries
- **Selectors**: Update CSS selectors if site structure changes

## Output

Files are saved to the `output/` directory:

- `medicare_plans.json` - Complete plan data in JSON format
- `medicare_plans.csv` - Flattened plan data in CSV format
- `medicare_plans_incremental.json` - Incrementally saved data
- `errors.json` - Failed zipcodes for retry

## Input

The crawler reads zipcodes from `ZipCodes.csv` with the following format:

```csv
State;City;Zip Code
Missouri;St. Louis;63101
Illinois;Chicago;60601
```

## Data Extracted

For each plan, the crawler extracts:

- Plan name and ID
- Monthly premium and deductible
- Star ratings
- Coverage details (dental, vision, hearing, fitness)
- Network type
- Carrier information
- Additional benefits

## Troubleshooting

### Selectors not working

The Medicare website may update its structure. Update the selectors in `src/config.js` or `src/extractors.js` to match the current site.

### Getting blocked

Try adjusting delays in `src/config.js`:

```javascript
delays: {
    betweenActions: { min: 2000, max: 4000 },
    afterPageLoad: { min: 3000, max: 5000 }
}
```

### Debug mode

Set `headless: false` in `src/config.js` to watch the browser:

```javascript
headless: false
```

## License

MIT
