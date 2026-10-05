'use strict';

// Runs every test/*.test.js. Each exports an async function that throws on failure.

const fs = require('fs');
const path = require('path');

(async () => {
    let failed = 0;
    for (const file of fs.readdirSync(__dirname).filter(f => f.endsWith('.test.js')).sort()) {
        try {
            await require(path.join(__dirname, file))();
            console.log('ok   ' + file);
        } catch (error) {
            failed++;
            console.log('FAIL ' + file);
            console.log(error);
        }
    }
    process.exit(failed ? 1 : 0);
})();
