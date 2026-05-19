const { chromium } = require('playwright');

(async () => {
  const url = process.env.URL || 'http://localhost:3002/?dev_auth=1';
  console.log('Opening', url);
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on('console', msg => console.log('PAGE:', msg.type(), msg.text()));
  page.on('requestfailed', req => console.log('REQ_FAILED:', req.url(), req.failure()?.errorText));
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await page.waitForSelector('button:has-text("Export")', { timeout: 15000 });
    await page.click('button:has-text("Export")');
    await page.waitForTimeout(1000);
    try { await page.click('button:has-text("FETCH")', { timeout: 5000 }); } catch(e) {}
    await page.waitForSelector('#hot-export .handsontable', { timeout: 45000 });

    const headers = await page.$$eval('#hot-export .handsontable th', ths => ths.map(t => t.innerText.trim()));
    console.log('Headers:', headers.join('|'));
    const typeIndex = headers.findIndex(h => h.toLowerCase() === 'type');
    const vendorIndex = headers.findIndex(h => h.toLowerCase() === 'vendor');

    const results = {};

    async function testColumn(idx, name) {
      try {
        await page.evaluate((col) => {
          const row = document.querySelector('#hot-export .handsontable tbody tr');
          if (!row) return false;
          const cell = row.children[col];
          if (!cell) return false;
          cell.scrollIntoView();
          cell.click();
          return true;
        }, idx);

        // Wait for known Handsontable dropdown renderers. Give a slightly longer timeout
        const dropdownSel = '.htDropdownMenu, .ht_master, .htUISelect, .htContextMenu, .htAutocomplete';
        const appeared = await page.waitForSelector(dropdownSel, { timeout: 5000 }).then(() => true).catch(() => false);
        results[name] = { appeared };

        if (appeared) {
          // Wait briefly for the dropdown content to stabilize
          await page.waitForTimeout(150);
          // Try multiple selectors that Handsontable or UI components may render
          const itemSelectors = [
            '.htDropdownMenu .ht_master td',
            '.ht_master td',
            '.ht_master .htCore td',
            '.htUISelect option',
            '.htAutocomplete .ht_match',
            '.htAutocomplete li',
            '.htContextMenu li',
            '.htDropdownMenu td'
          ];
          const items = await page.evaluate((sels) => {
            const out = [];
            for (const sel of sels) {
              const nodes = Array.from(document.querySelectorAll(sel));
              for (const n of nodes) {
                const t = (n.innerText || n.textContent || '').trim();
                if (t) out.push(t);
              }
            }
            // dedupe while preserving order
            return Array.from(new Set(out));
          }, itemSelectors);
          results[name].items = items.slice(0, 50);
        }
        console.log(`${name} dropdown appeared: ${appeared}`);
      } catch (e) {
        console.error('Error testing', name, e.message);
        results[name] = { appeared: false };
      }
    }

    if (typeIndex >= 0) await testColumn(typeIndex, 'Type');
    if (vendorIndex >= 0) await testColumn(vendorIndex, 'Vendor');

    console.log('Results:', JSON.stringify(results, null, 2));
    await browser.close();
    process.exitCode = 0;
  } catch (e) {
    console.error('Test failed', e);
    await browser.close();
    process.exitCode = 1;
  }
})();
