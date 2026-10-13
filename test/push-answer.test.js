'use strict';

// Nest's answer to a pushed change is logged and nothing else: whatever the answer body holds, and whatever goes
// wrong describing it, the push is treated exactly as it was before the answer was looked at.
// Two answers are acted on since 4.6.10-ha.8, a bare status 4 and a bare status 7 (test/push-resend.test.js). Here
// they are given with that switched off (nest-no-resend.on in the storage directory): logged, and the push treated
// as before.

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const axios = require('axios');
// index.js installs Promise.delay, which pushUpdates uses
require('../index.js');
const Connection = require('../lib/nest-connection');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function makeConnection(lines, verbose) {
    const text = args => args.map(el => typeof el == 'string' ? el : JSON.stringify(el)).join(' ');
    const logger = {
        info: (...args) => lines.push('info ' + text(args)),
        debug: (...args) => lines.push('debug ' + text(args)),
        warn: (...args) => lines.push('warn ' + text(args)),
        error: (...args) => lines.push('error ' + text(args))
    };
    const conn = new Connection({}, logger, !!verbose, false);
    conn.connected = true;
    conn.token = 'token';
    conn.transport_url = 'https://transport.invalid';
    conn.pushUpdatesDebounced = () => {};
    conn.updateHomeKit = () => lines.push('resync');
    conn.auth = () => {
        lines.push('reauthentication');
        return Promise.resolve(true);
    };
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.T1'] } },
        where: { S1: { wheres: [] } },
        device: { T1: { target_temperature_type: 'range', can_heat: true, can_cool: true } },
        shared: { T1: { target_temperature_type: 'range', target_temperature_low: 20, target_temperature_high: 22 } }
    };
    conn.accessories = { T1: { device: { target_temperature_type: 'range', target_temperature_low: 21, target_temperature_high: 23 } } };
    return conn;
}

// One length-delimited or varint protobuf field (numbers and lengths below 128)
const bytes = (number, payload) => Buffer.concat([Buffer.from([(number << 3) | 2, payload.length]), Buffer.from(payload)]);
const varint = (number, value) => Buffer.from([number << 3, value]);

module.exports = async function() {
    const realAdapter = axios.defaults.adapter, realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    const noResend = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-no-resend-'));
    fs.writeFileSync(path.join(noResend, 'nest-no-resend.on'), '');
    try {
        // One write of both setpoints, pushed and answered with `answer` after `seconds`. Returns everything the push
        // leaves behind and what was logged.
        let conn, lines;
        const fresh = verbose => {
            lines = [];
            conn = makeConnection(lines, verbose);
        };
        const push = async (answer, seconds, sabotage) => {
            for (let i = 0; i < 200 && !conn.TraitMap; i++) {
                await sleep(10);
            }
            lines.length = 0;
            const requests = [];
            axios.defaults.adapter = async config => {
                requests.push(config);
                now += (seconds || 1) * 1000;
                return Object.assign({ status: 200, statusText: 'OK', headers: {}, config: config }, answer);
            };
            if (sabotage) {
                sabotage(conn);
            }
            conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
            conn.commitUpdate('shared.T1', { target_temperature_high: 23 }, 'range', true);
            const result = await conn.pushUpdates();
            const left = {
                result: result,
                requests: requests.length,
                responseType: requests[0].responseType,
                failedPushAPICalls: conn.failedPushAPICalls,
                pending: conn.pendingUpdates.length,
                merge: conn.mergeUpdates.map(update => ({ value: update.object.value, pushed: update.pushed, holdsFor: update.expiry_time - now })),
                forced: conn.mergePendingUpdates(conn.currentState).shared.T1,
                other: lines.filter(line => !/^(info|debug) Nest answered the change|^debug /.test(line))
            };
            const answered = lines.filter(line => /Nest answered the change|could not be described/.test(line));
            clearTimeout(conn.mergeEndTimer);
            conn.mergeUpdates = [];
            return { left: left, answered: answered };
        };

        // What a successful push leaves behind: one request, both setpoints held for the 300 s echo wait, nothing queued,
        // nothing logged but the answer
        const expected = {
            result: undefined, requests: 1, responseType: 'arraybuffer', failedPushAPICalls: 0, pending: 0,
            merge: [{ value: { target_temperature_low: 21 }, pushed: true, holdsFor: 300000 }, { value: { target_temperature_high: 23 }, pushed: true, holdsFor: 300000 }],
            forced: { target_temperature_type: 'range', target_temperature_low: 21, target_temperature_high: 23 },
            other: []
        };
        const CHANGE = 'Nest answered the change shared.T1 {"target_temperature_low":21,"target_temperature_high":23} after ';
        // A push answered with a bare status 4 or 7, with sending it again switched off: one more line says so
        const pushNoResend = async (answer, seconds) => {
            conn.storagePath = noResend;
            try {
                return await push(answer, seconds);
            } finally {
                conn.storagePath = null;
            }
        };
        const notSentAgain = (seconds, status) => 'info Nest did not apply the change shared.T1 {"target_temperature_low":21,"target_temperature_high":23} (status ' + (status || 4) + ' after ' + seconds + ' s) - not sending it again: nest-no-resend.on exists in ' + noResend + '.';

        // An empty answer at normal speed: logged at info level, every time. (Before 4.6.10-ha.9 only the first of its
        // kind since the start, as a sample, and after that only with Debug.Verbose.)
        fresh(false);
        let run = await push({ data: Buffer.alloc(0) }, 3);
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '3 s: HTTP 200, 0 byte(s), empty.']);
        run = await push({ data: Buffer.alloc(0) }, 3);
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '3 s: HTTP 200, 0 byte(s), empty.'], 'at info level for a normal answer too');
        fresh(true);
        await push({ data: Buffer.alloc(0) }, 3);
        run = await push({ data: Buffer.alloc(0) }, 3);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '3 s: HTTP 200, 0 byte(s), empty.'], 'and so with Debug.Verbose');
        // (Those that would not have been logged go through conn.evidence: a version compared with one before
        // 4.6.10-ha.9 can leave them out)
        fresh(false);
        await push({ data: Buffer.alloc(0) }, 3);
        run = await push({ data: Buffer.alloc(0) }, 3, conn => {
            conn.evidence = () => {};
        });
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, []);

        // Slow: logged every time
        fresh(false);
        await push({ data: Buffer.alloc(0) }, 3);
        for (let i = 0; i < 2; i++) {
            run = await push({ data: Buffer.alloc(0) }, 25);
            assert.deepStrictEqual(run.left, expected);
            assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '25 s (slow): HTTP 200, 0 byte(s), empty.']);
        }
        run = await push({ data: Buffer.alloc(0) }, 9.9);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '9.9 s: HTTP 200, 0 byte(s), empty.']);
        run = await push({ data: Buffer.alloc(0) }, 10);
        assert.strictEqual(run.answered.length, 1);

        // An answer as the plugin's schema guesses it (BatchTraitOperation: one operation per change, progress COMPLETE):
        // logged every time
        const operation = (progress, status) => bytes(1, Buffer.concat([bytes(1, bytes(1, 'DEVICE_T1')), varint(2, progress)].concat(status === undefined ? [] : [bytes(3, status)])));
        run = await push({ data: operation(4) }, 2);
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '2 s: HTTP 200, 17 byte(s), operation (progress 4, status none), starts 0a0f0a0b0a094445564943455f54311004.']);
        run = await push({ data: operation(4) }, 2);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '2 s: HTTP 200, 17 byte(s), operation (progress 4, status none), starts 0a0f0a0b0a094445564943455f54311004.']);
        // ... with an OK status (code 0 is not sent): a kind of its own, the same the second time
        const first = await push({ data: operation(4, Buffer.alloc(0)) }, 2);
        run = await push({ data: operation(4, Buffer.alloc(0)) }, 2);
        assert.strictEqual(run.answered.length, 1);
        assert.ok(/^info .* after 2 s: HTTP 200, 19 byte\(s\), operation \(progress 4, status 0\), starts /.test(run.answered[0]), run.answered[0]);
        assert.deepStrictEqual(run.answered, first.answered);

        // A status other than 0, in an operation or as the observe stream sends one: logged every time, and the push is
        // still a success. No real answer has been seen: a body read wrongly must not make a landed write look failed.
        for (let i = 0; i < 2; i++) {
            run = await push({ data: operation(2, varint(1, 13)) }, 2);
            assert.deepStrictEqual(run.left, expected);
            assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '2 s: HTTP 200, 21 byte(s), operation (progress 2, status 13), starts 0a130a0b0a094445564943455f543110021a02080d.']);
            run = await pushNoResend({ data: bytes(2, varint(1, 4)) }, 17);
            assert.deepStrictEqual(run.left, Object.assign({}, expected, { other: [notSentAgain(17)] }));
            assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '17 s (slow): HTTP 200, 4 byte(s), status 4, starts 12020804.']);
            run = await pushNoResend({ data: conn.StreamBody.encode(conn.StreamBody.fromObject({ status: { code: 7, message: 'authentication failed' } })).finish() }, 2);
            assert.deepStrictEqual(run.left, Object.assign({}, expected, { other: [notSentAgain(2, 7)] }));
            assert.strictEqual(run.answered.length, 1);
            assert.ok(/ 27 byte\(s\), status 7, starts 12190807/.test(run.answered[0]), run.answered[0]);
            assert.ok(!/authentication failed/.test(run.answered[0]), 'only codes, sizes and the first bytes are logged');
        }

        // Garbage, a body that is not protobuf, text, an HTML page: logged every time, the push is a success
        for (const data of [Buffer.from([0xff, 0xff, 0xff, 0xff]), Buffer.from('<html><body>502 Bad Gateway</body></html>'), Buffer.from([0x0a, 0x7f, 0x01]), '{"error":"x"}', Buffer.from([0x08])]) {
            for (let i = 0; i < 2; i++) {
                run = await push({ data: data }, 2);
                assert.deepStrictEqual(run.left, expected);
                assert.strictEqual(run.answered.length, 1, 'logged at info level: ' + run.answered.join());
                assert.ok(/^info .* after 2 s: HTTP 200, \d+ byte\(s\), not readable \(.*\), starts [0-9a-f]+\.$/.test(run.answered[0]), run.answered[0]);
            }
        }

        // A huge body: not read, only its size and first bytes; never more than 32 bytes of any body
        const huge = Buffer.alloc(8 * 1024 * 1024, 0x61);
        run = await push({ data: huge }, 2);
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '2 s: HTTP 200, 8388608 byte(s), too large to read, starts ' + '61'.repeat(32) + '.']);
        // Many fields: the description stays short
        run = await push({ data: Buffer.concat(Array.from({ length: 5000 }, () => operation(4))) }, 2);
        assert.deepStrictEqual(run.left, expected);
        assert.ok(run.answered[0].length < 500, run.answered[0]);

        // No body at all, or something that is not bytes
        for (const answer of [{}, { data: null }, { data: '' }, { data: new ArrayBuffer(0) }, { data: new Uint8Array(0) }]) {
            run = await push(answer, 2);
            assert.deepStrictEqual(run.left, expected);
            assert.deepStrictEqual(run.answered, ['info ' + CHANGE + '2 s: HTTP 200, 0 byte(s), empty.'], 'empty, as seen before');
        }
        run = await pushNoResend({ data: new Uint8Array([0x12, 0x02, 0x08, 0x04]) }, 2);
        assert.deepStrictEqual(run.left, Object.assign({}, expected, { other: [notSentAgain(2)] }));
        assert.ok(/4 byte\(s\), status 4, starts 12020804\.$/.test(run.answered[0]), run.answered[0]);
        for (const data of [{ some: 'object' }, 42, true, () => {}]) {
            run = await push({ data: data }, 2);
            assert.deepStrictEqual(run.left, expected);
            assert.ok(/HTTP 200, not bytes, a value of type (object|number|boolean|function)\.$/.test(run.answered[0]), run.answered.join());
        }
        run = await push({ data: Buffer.alloc(0), status: 204 }, 2);
        assert.deepStrictEqual(run.left, expected);

        // One line per answer, however many kinds there are; the kinds are remembered only up to a limit
        fresh(false);
        let logged = 0;
        for (let i = 0; i < 40; i++) {
            run = await push({ data: Buffer.concat(Array.from({ length: i % 20 + 1 }, () => operation(4))).subarray(0, 17 * 8) }, 2);
            logged += run.answered.length;
        }
        assert.strictEqual(logged, 40);
        assert.ok(conn.pushAnswerKinds.length <= 16);

        // Something going wrong while the answer is described or logged changes nothing: an answer whose data cannot
        // even be looked at, a logger that throws, the describing function itself throwing
        fresh(true);
        const poisoned = new Proxy({}, { getPrototypeOf() { throw new Error('unreadable'); } });
        run = await push({ data: poisoned }, 2);
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, ['debug Nest\'s answer to a change could not be described: unreadable']);
        fresh(false);
        const failingLogger = (...args) => {
            if (/Nest answered|could not be described/.test(String(args[0]))) {
                throw new Error('logger failed');
            }
        };
        run = await push({ data: Buffer.from([0x12, 0x02, 0x08, 0x0d]) }, 20, conn => {
            conn.log = failingLogger;
        });
        assert.deepStrictEqual(run.left, expected);
        fresh(false);
        run = await push({ data: Buffer.from([0x12, 0x02, 0x08, 0x0d]) }, 20, conn => {
            conn.log = conn.verbose = failingLogger;
        });
        assert.deepStrictEqual(run.left, expected);
        fresh(false);
        await push({ data: Buffer.alloc(0) }, 2);
        run = await push({ data: Buffer.alloc(0) }, 2, conn => {
            conn.evidence = conn.verbose = failingLogger;
        });
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, []);
        fresh(false);
        run = await push({ data: Buffer.from([0x12, 0x02, 0x08, 0x0d]) }, 20, conn => {
            conn.logPushAnswer = () => {
                throw new Error('describing failed');
            };
        });
        assert.deepStrictEqual(run.left, expected);
        fresh(false);
        run = await push({ data: Buffer.alloc(0) }, 2, conn => {
            conn.pushAnswerKinds = null;
        });
        assert.deepStrictEqual(run.left, expected);
        assert.deepStrictEqual(run.answered, [], 'the failure itself is only said with Debug.Verbose');

        // A push with a REST change too: the line is about the protobuf request, and names its changes only
        fresh(false);
        for (let i = 0; i < 200 && !conn.TraitMap; i++) {
            await sleep(10);
        }
        const urls = [];
        axios.defaults.adapter = async config => {
            urls.push(config.url.replace(/^https:\/\/[^/]+/, ''));
            return { data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config };
        };
        conn.commitUpdate('device.T9', { fan_timer_timeout: 0 }, null, false);
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        await conn.pushUpdates();
        assert.deepStrictEqual(urls, ['/v5/put', '/nestlabs.gateway.v1.TraitBatchApi/BatchUpdateState']);
        assert.deepStrictEqual(lines.filter(line => /Nest answered/.test(line)), ['info Nest answered the change shared.T1 {"target_temperature_low":21} after 0 s: HTTP 200, 0 byte(s), empty.']);
        clearTimeout(conn.mergeEndTimer);

        // A push that fails is handled as before: a 400 with a body of bytes is not retried, the change is dropped and
        // said to be lost; a 401 is retried after reauthentication
        fresh(false);
        for (let i = 0; i < 200 && !conn.TraitMap; i++) {
            await sleep(10);
        }
        const failing = status => {
            let calls = 0;
            axios.defaults.adapter = async config => {
                if (calls++) {
                    return { data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config };
                }
                const error = new Error('Request failed with status code ' + status);
                error.response = { status: status, headers: {}, data: Buffer.from([0x12, 0x02, 0x08, 0x07]) };
                error.config = config;
                throw error;
            };
            return () => calls;
        };
        let calls = failing(400);
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        await conn.pushUpdates();
        assert.strictEqual(calls(), 1);
        assert.strictEqual(conn.mergeUpdates.length, 0, 'dropped');
        assert.ok(lines.some(line => /^error .*HTTP 400.* - these changes were NOT applied: shared\.T1 \{"target_temperature_low":21\}/.test(line)), lines.join('\n'));
        assert.ok(!lines.some(line => /Nest answered the change/.test(line)), 'a failed push has no answer line');
        lines.length = 0;
        calls = failing(401);
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        await conn.pushUpdates();
        assert.strictEqual(calls(), 2, 'retried');
        assert.ok(lines.includes('reauthentication'));
        assert.strictEqual(conn.mergeUpdates.length, 1);
        assert.strictEqual(conn.mergeUpdates[0].pushed, true);
        assert.strictEqual(conn.failedPushAPICalls, 0);
        clearTimeout(conn.mergeEndTimer);
    } finally {
        axios.defaults.adapter = realAdapter;
        Date.now = realNow;
        fs.rmSync(noResend, { recursive: true, force: true });
    }
};
