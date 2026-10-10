'use strict';

// A newer change to a setting, once pushed, supersedes an earlier pushed change to the same setting (same object, same
// key), whether an earlier push carried that one or the same push did.
// 2026-10-10 02:01-02:06 PT: a controller wrote the upstairs pair 70/73 degF and, 60 s later, 69/72. The cool
// setpoint went 22.2 -> 22.8 -> 22.2 C; Nest reported only 22.2. That released the second write (its echo) but not
// the first (22.2 was its "value before": keep waiting), which went on forcing 22.8 into HomeKit until its 300 s
// refresh, while the thermostat held 22.2.

const assert = require('assert');
const axios = require('axios');
// index.js installs Promise.delay, which pushUpdates uses
require('../index.js');
const Connection = require('../lib/nest-connection');

const SETPOINTS = 'type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait';
const C = f => (f - 32) / 1.8;

async function withClock(fn) {
    const realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    const clock = { advance(ms) { now += ms; }, get now() { return now; } };
    try {
        return await fn(clock);
    } finally {
        Date.now = realNow;
    }
}

const tick = () => new Promise(resolve => setImmediate(resolve));

// Two thermostats, both at 68/72 degF
function makeConnection() {
    const info = [], errors = [];
    const logger = { info(...args) { info.push(args.join(' ')); }, debug() {}, warn() {}, error(...args) { errors.push(args.join(' ')); } };
    const conn = new Connection({}, logger, false, false);
    conn.info = info;
    conn.errors = errors;
    conn.pushUpdatesDebounced = () => {};
    conn.connected = true;
    conn.token = 'token';
    conn.transport_url = 'https://transport.invalid';
    conn.auth = () => Promise.resolve(true);
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.T1', 'device.T2'] } },
        where: { S1: { wheres: [] } },
        device: {
            T1: { target_temperature_type: 'range', can_heat: true, can_cool: true, fan_timer_timeout: 0, eco: { mode: 'schedule' } },
            T2: { target_temperature_type: 'range', can_heat: true, can_cool: true }
        },
        shared: {
            T1: { target_temperature_type: 'range', target_temperature_low: C(68), target_temperature_high: C(72) },
            T2: { target_temperature_type: 'range', target_temperature_low: C(68), target_temperature_high: C(72) }
        }
    };
    conn.accessories = {
        T1: { device: { target_temperature_type: 'range', target_temperature_low: C(68), target_temperature_high: C(72) } },
        T2: { device: { target_temperature_type: 'range', target_temperature_low: C(68), target_temperature_high: C(72) } }
    };
    conn.resyncs = [];
    conn.updateHomeKit = data => conn.resyncs.push(data.devices.thermostats.T1.target_temperature_high);
    conn.restarts = 0;
    conn.observeStreamSeq = 1;
    conn.observeRestart = () => conn.restarts++;
    return conn;
}

async function waitForProto(conn) {
    for (let i = 0; i < 200 && !conn.TraitMap; i++) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(conn.TraitMap, 'protobuf definitions loaded');
}

// What HomeKit is shown
const shown = (conn, id) => conn.apiResponseToObjectTree(conn.mergePendingUpdates(conn.currentState)).devices.thermostats[id || 'T1'];
const high = (conn, id) => shown(conn, id).target_temperature_high;
const low = (conn, id) => shown(conn, id).target_temperature_low;
const near = (actual, expected, message) => assert.ok(Math.abs(actual - expected) < 0.01, (message || 'value') + ': ' + actual + ', expected ' + expected);
const pending = conn => conn.mergeUpdates.map(update => update.object.object_key + ' ' + JSON.stringify(update.object.value));

// One HomeKit write of both setpoints (two characteristic writes, low first as on 2026-10-10), then the debounced push
async function writeRange(conn, id, lowF, highF) {
    conn.update('shared.' + id, 'target_temperature_low', C(lowF), 'range', true);
    conn.update('shared.' + id, 'target_temperature_high', C(highF), 'range', true);
    await conn.pushUpdates();
}

// A change and what its push does to the merge cache: the queue is emptied when the push starts (so the queued object
// is no longer shared with the next change), and the changes are settled when it ends
function write(conn, clock, key, body, success) {
    conn.commitUpdate(key, body, 'range', true);
    conn.pendingUpdates = [];
    conn.settleMergeUpdates(clock.now, success !== false);
}

// A frame from Nest (the caller has put its values into currentState)
function frame(conn) {
    conn.releaseEchoedMergeUpdates(conn.currentState);
}

// A new observe stream's first message: Nest's full current state
function snapshot(conn, traits) {
    conn.observeStreamSeq++;
    conn.releaseEchoedMergeUpdates(conn.currentState, conn.refreshSnapshotTraits({ hasDeviceInfo: true, traits: traits }));
}

const SNAPSHOT_T1 = [['target_temperature_settings', 'DEVICE_T1', SETPOINTS], ['hvac_control', 'DEVICE_T1', 'x']];

// Runs the merge timer's work at the present (mocked) time
async function due(conn) {
    conn.scheduleMergeEnd();
    await tick();
}

function done(conn) {
    clearTimeout(conn.mergeEndTimer);
}

const HIGH = 'target_temperature_high', LOW = 'target_temperature_low';

module.exports = async function() {
    const realAdapter = axios.defaults.adapter;
    try {
        axios.defaults.adapter = async config => ({ data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config });

        // The live case, through the real write and push path: 68/72 -> 70/73, 60 s later -> 69/72, and Nest reports
        // only 69/72. HomeKit must show 72 at once and keep it; the first write starts no refresh at its limit.
        await withClock(async clock => {
            const conn = makeConnection();
            await waitForProto(conn);
            await writeRange(conn, 'T1', 70, 73);
            near(high(conn), C(73));
            clock.advance(60000);
            await writeRange(conn, 'T1', 69, 72);
            near(high(conn), C(72));
            near(low(conn), C(69));
            clock.advance(20000);
            conn.currentState.shared.T1.target_temperature_low = C(69);
            frame(conn); // Nest never reports the intermediate 70/73
            near(high(conn), C(72), 'cool setpoint after Nest reports 69/72');
            near(low(conn), C(69), 'heat setpoint after Nest reports 69/72');
            assert.deepStrictEqual(pending(conn), [], 'nothing left forcing');
            clock.advance(221000); // 301 s after the first write's push
            await due(conn);
            assert.strictEqual(conn.restarts, 0, 'no echo refresh for the superseded write');
            assert.ok(!conn.info.some(line => /has not echoed/.test(line)), conn.info.join('\n'));
            near(high(conn), C(72));
            assert.ok(conn.resyncs.every(value => Math.abs(value - C(72)) < 0.01), 'HomeKit never re-synced to 73: ' + conn.resyncs);
            assert.strictEqual(conn.mergeEndTimer, null);
        });

        // The same with single changes of the cool setpoint, and the orders Nest's reports can take.
        // A = 73 (from 72), then B = 72 (back) or B = 74, pushed 60 s apart.
        const twoWrites = (clock, conn, secondF) => {
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            clock.advance(60000);
            write(conn, clock, 'shared.T1', { [HIGH]: C(secondF) });
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(secondF) })], 'the earlier change is superseded');
            near(high(conn), C(secondF));
        };

        // Nest reports only B
        await withClock(async clock => {
            const conn = makeConnection();
            twoWrites(clock, conn, 72);
            frame(conn); // 72: B's echo (also the value before A)
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(72));
            assert.strictEqual(conn.mergeEndTimer, null);
        });

        // Nest reports A late, then B: A's echo neither releases B nor shows as someone else's change
        for (const secondF of [72, 74]) {
            await withClock(async clock => {
                const conn = makeConnection();
                twoWrites(clock, conn, secondF);
                conn.currentState.shared.T1.target_temperature_high = C(73);
                frame(conn);
                near(high(conn), C(secondF), 'B still shown while Nest reports A');
                assert.strictEqual(conn.mergeUpdates.length, 1);
                conn.currentState.shared.T1.target_temperature_high = C(secondF);
                frame(conn);
                assert.deepStrictEqual(pending(conn), []);
                near(high(conn), C(secondF));
            });
        }

        // Nest still reports the value before A, then B. With B = 74 the stale 72 is waited out; with B = 72 it cannot
        // be told from B's echo (covered above: released, and 72 is what HomeKit shows either way).
        await withClock(async clock => {
            const conn = makeConnection();
            twoWrites(clock, conn, 74);
            frame(conn); // still 72
            near(high(conn), C(74));
            assert.strictEqual(conn.mergeUpdates.length, 1);
            conn.currentState.shared.T1.target_temperature_high = C(74);
            frame(conn);
            assert.deepStrictEqual(pending(conn), []);
            done(conn);
        });

        // Nest reports A, then nothing: B holds until ITS limit (300 s from its push), then the refresh decides. No
        // refresh at A's limit.
        await withClock(async clock => {
            const conn = makeConnection();
            twoWrites(clock, conn, 72);
            conn.currentState.shared.T1.target_temperature_high = C(73);
            frame(conn);
            clock.advance(241000); // 301 s after A's push
            await due(conn);
            assert.strictEqual(conn.restarts, 0, 'no refresh at the superseded change\'s limit');
            near(high(conn), C(72));
            clock.advance(60000); // 301 s after B's push
            await due(conn);
            assert.strictEqual(conn.restarts, 1, 'B\'s own refresh');
            assert.ok(conn.info.some(line => line.includes('Nest has not echoed shared.T1 ' + JSON.stringify({ [HIGH]: C(72) }))), conn.info.join('\n'));
            near(high(conn), C(72), 'still forced while the refresh is under way');
            snapshot(conn, SNAPSHOT_T1); // Nest has 73: B did not land
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(73), 'Nest\'s value');
            assert.strictEqual(conn.echoRefresh, null);
        });

        // Nest reports nothing: one refresh, at B's limit; here it finds B
        await withClock(async clock => {
            const conn = makeConnection();
            twoWrites(clock, conn, 74);
            clock.advance(241000);
            await due(conn);
            assert.strictEqual(conn.restarts, 0);
            near(high(conn), C(74));
            clock.advance(60000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            conn.currentState.shared.T1.target_temperature_high = C(74);
            snapshot(conn, SNAPSHOT_T1);
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(74));
            assert.deepStrictEqual(conn.resyncs, []);
        });

        // Someone else's change (app, schedule) after ours: Nest's value wins at once
        await withClock(async clock => {
            const conn = makeConnection();
            twoWrites(clock, conn, 72);
            conn.currentState.shared.T1.target_temperature_high = C(75);
            frame(conn);
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(75));
        });

        // A newer change supersedes only once its push has succeeded. Not yet pushed: both are kept, HomeKit shows the
        // newer. Its push fails: it is dropped at once and the earlier change, which did reach Nest, holds as before
        // (HomeKit must not fall back to the cached value from before it).
        axios.defaults.adapter = async config => {
            const error = new Error('Request failed with status code 400');
            error.response = { status: 400, headers: {}, data: '' };
            error.config = config;
            throw error;
        };
        await withClock(async clock => {
            const conn = makeConnection();
            await waitForProto(conn);
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            clock.advance(60000);
            conn.update('shared.T1', HIGH, C(72), 'range', true);
            assert.strictEqual(conn.mergeUpdates.length, 2, 'not superseded before the push');
            near(high(conn), C(72), 'the newer, unpushed change shows');
            await conn.pushUpdates();
            assert.ok(conn.errors.some(line => /NOT applied: shared\.T1/.test(line)), conn.errors.join('\n'));
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(73) })], 'the failed change is dropped, the earlier one kept');
            near(high(conn), C(73), 'the earlier write still holds');
            near(conn.resyncs[conn.resyncs.length - 1], C(73), 'HomeKit re-synced to it');
            frame(conn); // Nest still reports 72, the value before the earlier write
            near(high(conn), C(73));
            clock.advance(241000); // the earlier write's own limit
            await due(conn);
            assert.strictEqual(conn.restarts, 1, 'the earlier write keeps its own refresh');
            conn.currentState.shared.T1.target_temperature_high = C(73);
            snapshot(conn, SNAPSHOT_T1);
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(73));
        });
        axios.defaults.adapter = async config => ({ data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config });

        // Per key: an earlier change of both setpoints, a newer one of the cool setpoint only. The earlier one keeps
        // waiting (and refreshing) for its heat setpoint. The object queued for its push is not altered.
        await withClock(async clock => {
            const conn = makeConnection();
            conn.commitUpdate('shared.T1', { [LOW]: C(70), [HIGH]: C(73) }, 'range', true);
            const queued = conn.pendingUpdates[0].object;
            conn.pendingUpdates = [];
            conn.settleMergeUpdates(clock.now, true);
            const expiry = conn.mergeUpdates[0].expiry_time;
            clock.advance(60000);
            write(conn, clock, 'shared.T1', { [HIGH]: C(72) });
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [LOW]: C(70) }), 'shared.T1 ' + JSON.stringify({ [HIGH]: C(72) })]);
            assert.deepStrictEqual(queued.value, { [LOW]: C(70), [HIGH]: C(73) }, 'the queued object is untouched');
            assert.strictEqual(conn.mergeUpdates[0].expiry_time, expiry, 'the earlier change keeps its own limit');
            frame(conn); // Nest reports 68/72: the newer change's echo, the heat setpoint not yet
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [LOW]: C(70) })]);
            near(low(conn), C(70), 'heat setpoint still held');
            near(high(conn), C(72));
            clock.advance(241000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            assert.ok(conn.info.some(line => line.includes('Nest has not echoed shared.T1 ' + JSON.stringify({ [LOW]: C(70) }) + ' -')), conn.info.join('\n'));
            conn.currentState.shared.T1.target_temperature_low = C(70);
            snapshot(conn, SNAPSHOT_T1);
            assert.deepStrictEqual(pending(conn), []);
            near(low(conn), C(70));
        });

        // An earlier change in its echo refresh when the newer one is pushed: the refresh ends with it. The restarted
        // stream's snapshot (read as the newer change was pushed) does not decide the newer change, whose limit
        // counts from its own push.
        await withClock(async clock => {
            const conn = makeConnection();
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            clock.advance(301000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            assert.notStrictEqual(conn.echoRefresh, null);
            clock.advance(5000);
            write(conn, clock, 'shared.T1', { [HIGH]: C(74) });
            const pushed = clock.now;
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(74) })]);
            assert.strictEqual(conn.echoRefresh, null, 'the refresh ended with its only change');
            assert.ok(!conn.mergeUpdates[0].refreshing);
            assert.strictEqual(conn.mergeUpdates[0].expiry_time, pushed + 300000);
            snapshot(conn, SNAPSHOT_T1); // the new stream opens with 72: Nest has applied neither yet
            near(high(conn), C(74), 'not decided by the superseded change\'s snapshot');
            conn.currentState.shared.T1.target_temperature_high = C(73);
            snapshot(conn, SNAPSHOT_T1); // or with the earlier write
            near(high(conn), C(74));
            assert.strictEqual(conn.mergeUpdates.length, 1);
            clock.advance(31000); // the superseded change's refresh timeout
            await due(conn);
            assert.ok(!conn.info.some(line => /No current state from Nest/.test(line)), conn.info.join('\n'));
            near(high(conn), C(74));
            clock.advance(268000); // 299 s after the newer push
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            clock.advance(2000);
            await due(conn);
            assert.strictEqual(conn.restarts, 2, 'the newer change\'s own refresh, 300 s after its push');
            done(conn);
        });

        // In its refresh and superseded in part: it stays in the refresh for the rest
        await withClock(async clock => {
            const conn = makeConnection();
            write(conn, clock, 'shared.T1', { [LOW]: C(70), [HIGH]: C(73) });
            clock.advance(301000);
            await due(conn);
            write(conn, clock, 'shared.T1', { [HIGH]: C(74) });
            assert.strictEqual(conn.mergeUpdates[0].refreshing, true);
            assert.notStrictEqual(conn.echoRefresh, null);
            snapshot(conn, SNAPSHOT_T1); // Nest has 68/72: the heat setpoint did not land
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(74) })]);
            near(low(conn), C(68));
            near(high(conn), C(74));
            assert.strictEqual(conn.echoRefresh, null);
            done(conn);
        });

        // Per object: the same setting of another thermostat is unrelated
        await withClock(async clock => {
            const conn = makeConnection();
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            const expiry = conn.mergeUpdates[0].expiry_time;
            clock.advance(60000);
            write(conn, clock, 'shared.T2', { [HIGH]: C(72) });
            assert.strictEqual(conn.mergeUpdates.length, 2);
            assert.strictEqual(conn.mergeUpdates[0].expiry_time, expiry);
            frame(conn); // T2's 72 is its echo; T1's 72 is the value before its write
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(73) })]);
            near(high(conn), C(73), 'T1 still held');
            clock.advance(241000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1, 'T1\'s own refresh, at its own limit');
            done(conn);
        });

        // Per key: a later push of another setting of the same thermostat leaves the earlier change alone
        await withClock(async clock => {
            const conn = makeConnection();
            write(conn, clock, 'shared.T1', { [LOW]: C(70) });
            const expiry = conn.mergeUpdates[0].expiry_time;
            clock.advance(60000);
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            assert.strictEqual(conn.mergeUpdates.length, 2);
            assert.strictEqual(conn.mergeUpdates[0].expiry_time, expiry);
            frame(conn);
            near(low(conn), C(70));
            near(high(conn), C(73));
            clock.advance(241000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            assert.deepStrictEqual(conn.mergeUpdates.map(update => !!update.refreshing), [true, true], 'both in one refresh, as before');
            done(conn);
        });

        // A single change is untouched: it holds until its echo
        await withClock(async clock => {
            const conn = makeConnection();
            write(conn, clock, 'shared.T1', { [HIGH]: C(73) });
            clock.advance(120000);
            frame(conn);
            near(high(conn), C(73));
            assert.strictEqual(conn.mergeUpdates.length, 1);
            conn.currentState.shared.T1.target_temperature_high = C(73);
            frame(conn);
            assert.deepStrictEqual(pending(conn), []);
        });

        // Mode changes go the same way: range -> heat -> range within the echo delay, and Nest reports only range.
        // Each writes device.eco and shared { target_change_pending, target_temperature_type }.
        await withClock(async clock => {
            const conn = makeConnection();
            conn.pushUpdates = () => {}; // mode changes push at once
            conn.update('shared.T1', 'hvac_mode', 'heat', 'range', true);
            conn.settleMergeUpdates(clock.now, true);
            clock.advance(60000);
            conn.update('shared.T1', 'hvac_mode', 'range', 'heat', true);
            assert.strictEqual(conn.mergeUpdates.length, 4, 'not superseded before the push');
            conn.settleMergeUpdates(clock.now, true);
            assert.deepStrictEqual(pending(conn), ['device.T1 ' + JSON.stringify({ eco: { mode: 'schedule' } }), 'shared.T1 ' + JSON.stringify({ target_change_pending: true, target_temperature_type: 'range' })]);
            frame(conn);
            assert.deepStrictEqual(pending(conn), []);
            assert.strictEqual(conn.mergePendingUpdates(conn.currentState).shared.T1.target_temperature_type, 'range');
            clock.advance(241000);
            await due(conn);
            assert.strictEqual(conn.restarts, 0);
        });

        // And the fan: on, then off, and Nest reports only off
        await withClock(async clock => {
            const conn = makeConnection();
            conn.update('device.T1', 'fan_timer_active', true, 'range', true);
            conn.pendingUpdates = [];
            conn.settleMergeUpdates(clock.now, true);
            assert.ok(conn.mergePendingUpdates(conn.currentState).device.T1.fan_timer_timeout > 0);
            clock.advance(60000);
            conn.update('device.T1', 'fan_timer_active', false, 'range', true);
            conn.pendingUpdates = [];
            conn.settleMergeUpdates(clock.now, true);
            frame(conn);
            assert.deepStrictEqual(pending(conn), []);
            assert.strictEqual(conn.mergePendingUpdates(conn.currentState).device.T1.fan_timer_timeout, 0);
        });

        // The remaining tests go through the real update() and pushUpdates(), HTTP stubbed
        let requests = 0;
        const succeed = async config => {
            requests++;
            return { data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config };
        };
        const fail = status => async config => {
            requests++;
            const error = new Error('Request failed with status code ' + status);
            error.response = { status: status, headers: {}, data: '' };
            error.config = config;
            throw error;
        };
        const was = { [LOW]: C(68), [HIGH]: C(72) };

        // One write of both setpoints, both changed (68/72 -> 70/73; 2026-10-06 21:17 PT: 67/70 -> 69/74, and HomeKit
        // showed 67/74 three minutes later): two characteristic writes in the same second, one push. Each is its own
        // change with its own pre / pre_nest, so a frame before the echo (other traits; setpoints still 68/72)
        // releases neither. The change made first used to share the queued object: it took on the second key, for
        // which it had no pre, and that frame released it. In either order of the two writes.
        for (const order of [[LOW, HIGH], [HIGH, LOW]]) {
            await withClock(async clock => {
                axios.defaults.adapter = succeed;
                requests = 0;
                const conn = makeConnection();
                await waitForProto(conn);
                const to = { [LOW]: C(70), [HIGH]: C(73) };
                order.forEach(key => conn.update('shared.T1', key, to[key], 'range', true));
                assert.strictEqual(conn.pendingUpdates.length, 1, 'one queued object for the device');
                await conn.pushUpdates();
                assert.strictEqual(requests, 1, 'one push');
                assert.deepStrictEqual(conn.mergeUpdates.map(update => [update.object.value, update.pre, update.pre_nest]),
                    order.map(key => [{ [key]: to[key] }, { [key]: was[key] }, { [key]: was[key] }]), 'one change per setpoint, each with its own value before');
                clock.advance(20000);
                frame(conn);
                near(low(conn), C(70), 'heat setpoint after a frame before the echo (' + order[0] + ' written first)');
                near(high(conn), C(73), 'cool setpoint after a frame before the echo (' + order[0] + ' written first)');
                assert.strictEqual(conn.mergeUpdates.length, 2, 'both still held');
                Object.assign(conn.currentState.shared.T1, to);
                frame(conn); // the echo
                assert.deepStrictEqual(pending(conn), []);
                near(low(conn), C(70));
                near(high(conn), C(73));
                assert.deepStrictEqual(conn.resyncs, []);
                assert.strictEqual(conn.mergeEndTimer, null);
            });
        }

        // The same setting written twice before one push (73, then 72 or 74): only the second value is sent, so only
        // the second change is left. The first, never sent, must not be held or refreshed.
        for (const secondF of [72, 74]) {
            await withClock(async clock => {
                axios.defaults.adapter = succeed;
                requests = 0;
                const conn = makeConnection();
                await waitForProto(conn);
                conn.update('shared.T1', HIGH, C(73), 'range', true);
                conn.update('shared.T1', HIGH, C(secondF), 'range', true);
                assert.strictEqual(conn.mergeUpdates.length, 2, 'not superseded before the push');
                assert.deepStrictEqual(conn.pendingUpdates.map(el => el.object.value), [{ [HIGH]: C(secondF) }], 'the queue holds the last value only');
                await conn.pushUpdates();
                assert.strictEqual(requests, 1);
                assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(secondF) })]);
                near(high(conn), C(secondF));
                conn.currentState.shared.T1.target_temperature_high = C(secondF);
                frame(conn);
                assert.deepStrictEqual(pending(conn), []);
                near(high(conn), C(secondF));
                clock.advance(301000);
                await due(conn);
                assert.strictEqual(conn.restarts, 0);
                assert.ok(!conn.info.some(line => /has not echoed/.test(line)), conn.info.join('\n'));
                near(high(conn), C(secondF));
                assert.deepStrictEqual(conn.resyncs, []);
            });
        }

        // A push that fails retryably (401), a newer change to the same setting made during the reauthentication, and
        // one retry push that carries (and settles) both: the newer supersedes the earlier, as across two pushes.
        // 73, then back to 72: Nest reports 72, and nothing may go on holding 73.
        await withClock(async clock => {
            requests = 0;
            const ok = succeed, unauthorized = fail(401);
            axios.defaults.adapter = config => (requests == 0 ? unauthorized : ok)(config);
            const conn = makeConnection();
            await waitForProto(conn);
            conn.auth = () => {
                clock.advance(2000);
                conn.update('shared.T1', HIGH, C(72), 'range', true);
                return Promise.resolve(true);
            };
            conn.update('shared.T1', HIGH, C(73), 'range', true);
            await conn.pushUpdates();
            assert.strictEqual(requests, 2, 'the failed push and one retry');
            assert.strictEqual(conn.pendingUpdates.length, 0);
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(72) })], 'only the newer change is left');
            near(high(conn), C(72));
            clock.advance(20000);
            frame(conn); // Nest reports 72
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(72), 'cool setpoint after Nest reports 72');
            clock.advance(281000);
            await due(conn);
            assert.strictEqual(conn.restarts, 0);
            near(high(conn), C(72));
            assert.strictEqual(conn.mergeEndTimer, null);
        });

        // Nest reports B, then a late echo of A, then B again. B's echo ends the hold, so the late echo of A shows
        // as Nest's value (as it did before superseding: nothing tells it from someone else's change) until Nest
        // reports again.
        for (const secondF of [72, 74]) {
            await withClock(async clock => {
                axios.defaults.adapter = succeed;
                const conn = makeConnection();
                await waitForProto(conn);
                conn.update('shared.T1', HIGH, C(73), 'range', true);
                await conn.pushUpdates();
                clock.advance(60000);
                conn.update('shared.T1', HIGH, C(secondF), 'range', true);
                await conn.pushUpdates();
                conn.currentState.shared.T1.target_temperature_high = C(secondF);
                frame(conn);
                assert.deepStrictEqual(pending(conn), []);
                near(high(conn), C(secondF));
                conn.currentState.shared.T1.target_temperature_high = C(73);
                frame(conn);
                near(high(conn), C(73), 'the late echo of the earlier write shows');
                conn.currentState.shared.T1.target_temperature_high = C(secondF);
                frame(conn);
                near(high(conn), C(secondF));
                assert.strictEqual(conn.mergeEndTimer, null);
            });
        }

        // The earlier change is in its echo refresh when a newer change's push fails: the newer one is dropped, the
        // earlier one stays in its refresh and is decided by the snapshot
        await withClock(async clock => {
            axios.defaults.adapter = succeed;
            const conn = makeConnection();
            await waitForProto(conn);
            conn.update('shared.T1', HIGH, C(73), 'range', true);
            await conn.pushUpdates();
            clock.advance(301000);
            await due(conn);
            assert.strictEqual(conn.restarts, 1);
            const expiry = conn.mergeUpdates[0].expiry_time;
            clock.advance(5000);
            axios.defaults.adapter = fail(400);
            conn.update('shared.T1', HIGH, C(74), 'range', true);
            near(high(conn), C(74));
            await conn.pushUpdates();
            assert.deepStrictEqual(pending(conn), ['shared.T1 ' + JSON.stringify({ [HIGH]: C(73) })]);
            assert.strictEqual(conn.mergeUpdates[0].refreshing, true);
            assert.strictEqual(conn.mergeUpdates[0].expiry_time, expiry);
            assert.notStrictEqual(conn.echoRefresh, null);
            near(high(conn), C(73));
            near(conn.resyncs[conn.resyncs.length - 1], C(73), 'HomeKit re-synced to the earlier write');
            assert.strictEqual(conn.restarts, 1);
            conn.currentState.shared.T1.target_temperature_high = C(73);
            snapshot(conn, SNAPSHOT_T1);
            assert.deepStrictEqual(pending(conn), []);
            near(high(conn), C(73));
            assert.strictEqual(conn.echoRefresh, null);
            assert.strictEqual(conn.mergeEndTimer, null);
        });
    } finally {
        axios.defaults.adapter = realAdapter;
    }
};
