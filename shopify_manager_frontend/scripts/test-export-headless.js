const { chromium } = require('playwright');

(async () => {
  const url = process.env.URL || 'http://localhost:3002';
  console.log('Opening', url);
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    // Wait for sidebar button labelled Export
    await page.waitForSelector('button:has-text("Export")', { timeout: 15000 });
    await page.click('button:has-text("Export")');
    // Wait for Handsontable container
    await page.waitForSelector('#hot-export .handsontable', { timeout: 15000 });

    // Find header index for Type and Vendor
    const headers = await page.$$eval('#hot-export .handsontable th', ths => ths.map(t => t.innerText.trim()));
    console.log('Headers:', headers.join('|'));
    const typeIndex = headers.findIndex(h => h.toLowerCase() === 'type');
    const vendorIndex = headers.findIndex(h => h.toLowerCase() === 'vendor');

    if (typeIndex === -1 && vendorIndex === -1) {
      console.error('No Type/Vendor headers found');
      process.exitCode = 2;
      return;
    }

    const results = {};

    async function testColumn(idx, name) {
      try {
        // Click cell at row 0, column idx
        await page.evaluate((col) => {
          const row = document.querySelector('#hot-export .handsontable tbody tr');
          if (!row) return false;
          const cell = row.children[col];
          if (!cell) return false;
          cell.scrollIntoView();
          cell.click();
          return true;
        }, idx);

        // wait for any of the dropdown menus
        const dropdownSel = '.htDropdownMenu, .htUISelect, .htContextMenu, .htAutocomplete';
        const appeared = await page.waitForSelector(dropdownSel, { timeout: 3000 }).then(() => true).catch(() => false);
        results[name] = appeared;
        console.log(`${name} dropdown appeared: ${appeared}`);
      } catch (e) {
        console.error('Error testing', name, e.message);
        results[name] = false;
      }
    }

    if (typeIndex >= 0) await testColumn(typeIndex, 'Type');
    if (vendorIndex >= 0) await testColumn(vendorIndex, 'Vendor');

    console.log('Results:', results);
    await browser.close();
    process.exitCode = 0;
  } catch (e) {
    console.error('Test failed', e);
    await browser.close();
    process.exitCode = 1;
  }
})();
