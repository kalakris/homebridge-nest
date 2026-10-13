'use strict';

// The randomized test of a temperature sensor's two inputs (two-input-fuzz.js), on fixed seeds: 160 scenarios, in
// which each API delivers its readings in order, the stream late or never for some, the REST API sends objects again
// with nothing new, streams end and start, setpoints are written and reported; in 40 of them an older reading also
// arrives after a newer one. No rule R1 to R5 may be broken.
// Many more seeds, and the same seeds against another version of the plugin: see create() in two-input-fuzz.js.
// (4.6.10-ha.8 breaks R1 in nine of ten of these scenarios.)

const assert = require('assert');
const path = require('path');
const fuzz = require('./two-input-fuzz');

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
        for (let seed = 1; seed <= 160; seed++) {
            const result = await fuzzer.run(fuzz.generate('s' + seed, { racy: seed > 120 }));
            result.violations.forEach(el => broken.push('seed ' + seed + ': ' + el.rule + ' at ' + el.at + ' s: ' + el.text));
            for (const name in result.happened) {
                happened[name] = (happened[name] || 0) + result.happened[name];
            }
        }
    } finally {
        fuzzer.dispose();
    }
    assert.deepStrictEqual(broken, []);
    // The scenarios did go through what the test is about: the REST API first with a reading, a stream message about
    // something else while the stream's own reading is the older one, the REST API's object again while its reading
    // is behind, a new stream that brings a reading nobody had delivered, writes, a stream restarted for one, and (in
    // the 40) an older reading as news from either API
    const least = { restFirst: 500, staleCopy: 400, restRepeat: 300, restRepeatBehind: 5, snapshots: 150, snapshotNews: 20, writes: 200, refreshes: 10, K1: 10, K2: 10 };
    for (const name in least) {
        assert.ok(happened[name] >= least[name], name + ': ' + happened[name]);
    }
};
