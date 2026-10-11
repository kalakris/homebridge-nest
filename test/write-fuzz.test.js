'use strict';

// The randomized test of the write path (write-fuzz.js), on fixed seeds: 150 scenarios without failing requests and
// 50 with. No invariant I1 to I15 may be broken. With failing requests the plugin's older paths break some of them
// in ways this version did not introduce (reported as P2, P3, P8, P10: write-fuzz.js says what each is); those are
// not failures here. (So are P7 and P13.)
// Many more seeds, and the same seeds against another version of the plugin: see create() in write-fuzz.js.

const assert = require('assert');
const path = require('path');
const fuzz = require('./write-fuzz');

module.exports = async function() {
    const fuzzer = fuzz.create({
        Connection: require('../lib/nest-connection'),
        harness: require('./harness'),
        axios: require('axios'),
        protobuf: require('protobufjs'),
        protoPath: path.join(__dirname, '../lib/protobuf/root.proto')
    });
    await fuzzer.prepare();
    const broken = [], happened = {};
    try {
        // (and six seeds that reach two rarer paths: a value that is not the last one asked for, and a change that
        // went out with another's resend and is given up later)
        const seeds = Array.from({ length: 200 }, (el, index) => [index + 1, index >= 150]).concat([[284, false], [1016, false], [1431, false], [231, false], [476, false], [913, false]]);
        for (const [seed, errors] of seeds) {
            const result = await fuzzer.run(fuzz.generate('s' + seed, { errors: errors }));
            result.violations.filter(el => /^I/.test(el.invariant)).forEach(el => broken.push('seed ' + seed + ': ' + el.invariant + ' at ' + el.at + ' s: ' + el.text));
            for (const name in result.happened) {
                happened[name] = (happened[name] || 0) + result.happened[name];
            }
        }
    } finally {
        fuzzer.dispose();
    }
    assert.deepStrictEqual(broken, []);
    // The scenarios did go through what the test is about
    for (const name of ['resent', 'givenUp', 'notResent', 'switchedOff', 'lost', 'reauthFailed', 'status7', 'refreshes']) {
        assert.ok(happened[name] >= 20, name + ': ' + happened[name]);
    }
};
