'use strict';

// The randomized test of the write path (write-fuzz.js), on fixed seeds: 154 scenarios without failing requests and
// 56 with. No invariant I1 to I15 may be broken. With failing requests the plugin's older paths break some of them
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
        // (and ten seeds that reach rarer paths, picked for breaking a version of the plugin with that path taken
        // out: a change that went out with another's resend and is given up later; with failing requests, a value
        // that is not the last one asked for, and a newer value written into a change queued for its retry)
        const seeds = Array.from({ length: 200 }, (el, index) => [index + 1, index >= 150])
            .concat([231, 476, 913, 1431].map(seed => [seed, false]), [96, 103, 429, 3657, 3860, 4485].map(seed => [seed, true]));
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
    for (const name of ['resent', 'givenUp', 'notResent', 'switchedOff', 'lost', 'status7', 'refreshes']) {
        assert.ok(happened[name] >= 20, name + ': ' + happened[name]);
    }
};
