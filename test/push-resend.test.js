'use strict';

// A change Nest answers with a bare status 4 was not applied: it is sent again at once, once, and dropped (and said
// to be lost) if Nest answers that again. Never a value a newer change has replaced. Any other answer is taken as
// before, and so is that one while nest-no-resend.on exists in the storage directory.
// 2026-10-11 01:31:18Z: the downstairs pair was written (low 18.9, high 22.8), Nest answered after 15.6 s with
// HTTP 200 and the four bytes 12 02 08 04, and 300 s later its state was still 21.1.

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const axios = require('axios');
const varint = require('varint');
// index.js installs Promise.delay, which pushUpdates uses
require('../index.js');
const Connection = require('../lib/nest-connection');

// Two thermostats. The ids are as long as real ones, so that the rebuilt answer below has the real one's size.
const UP = 'AAAA000000000001', DN = 'BBBB000000000002';
const NAMES = { [UP]: 'up', [DN]: 'dn' };

// The answer to the lost write, whole, as logged: "HTTP 200, 4 byte(s), status 4, starts 12020804"
const NOT_APPLIED = Buffer.from('12020804', 'hex');

// The answer to a write that landed, 2026-10-11 00:10:06Z, one push for both thermostats: "HTTP 200, 223 byte(s),
// operation (progress none, status none), status 0, starts 0ada010a6b0a5c0a174445564943455f" followed by 16 bytes of
// a device id. Only those first 32 bytes were logged. They fit this shape, which is what is rebuilt here: field 1
// (218 bytes) holding one entry of 107 bytes per thermostat, each starting with a 92-byte object id (the device id,
// the trait's label and a uuid: 23 + 27 + 36 characters), and then a status without a code. The device ids are this
// test's and the 13 bytes after each object id are made up.
const field = (number, payload) => Buffer.concat([Buffer.from(varint.encode((number << 3) | 2)), Buffer.from(varint.encode(Buffer.byteLength(payload))), Buffer.from(payload)]);
const landed = ids => Buffer.concat([
    field(1, Buffer.concat(ids.map(id => field(1, Buffer.concat([
        field(1, Buffer.concat([field(1, 'DEVICE_' + id), field(2, 'target_temperature_settings'), field(3, '6f1e2d3c-4b5a-4978-8a6b-5c4d3e2f1a0b')])),
        Buffer.from('120b08dec5a9d60610808080' + '01', 'hex')
    ]))))),
    Buffer.from('1200', 'hex')
]);
const LANDED_BOTH = landed([DN, UP]);
const ANSWERED_BOTH = 'HTTP 200, 223 byte(s), operation (progress none, status none), status 0, starts 0ada010a6b0a5c0a174445564943455f' + Buffer.from(DN).toString('hex') + '.';

const realSetTimeout = setTimeout, realClearTimeout = clearTimeout;
const turn = () => new Promise(resolve => realSetTimeout(resolve, 1));

// Everything a case touches outside its connection: the clock, the timers it leaves, the delays pushUpdates asks for
// and the requests it sends (answered by the case, in the order and at the times it chooses)
const env = { now: 0, live: new Set(), world: null };

async function until(condition, what) {
    for (let i = 0; i < 500; i++) {
        if (condition()) {
            return;
        }
        await turn();
    }
    assert.fail('timed out waiting for ' + what);
}

// Long enough for anything a push would still do (its promises, and the zero delay before it sends)
async function quiet() {
    for (let i = 0; i < 20; i++) {
        await turn();
    }
}

async function makeWorld(storagePath) {
    const lines = [];
    const text = args => args.map(el => typeof el == 'string' ? el : JSON.stringify(el)).join(' ');
    const logger = {
        info: (...args) => lines.push('info ' + text(args)),
        debug() {},
        warn: (...args) => lines.push('warn ' + text(args)),
        error: (...args) => lines.push('error ' + text(args))
    };
    const conn = new Connection({}, logger, false, false, storagePath);
    conn.connected = true;
    conn.token = 'token';
    conn.transport_url = 'https://transport.invalid';
    conn.pushUpdatesDebounced = () => {};
    conn.auth = () => {
        lines.push('reauthentication');
        return Promise.resolve(true);
    };
    const thermostat = () => ({ target_temperature_type: 'range', can_heat: true, can_cool: true, eco: { mode: 'schedule' } });
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.' + UP, 'device.' + DN] } },
        where: { S1: { wheres: [] } },
        device: { [UP]: thermostat(), [DN]: thermostat() },
        shared: {
            [UP]: { target_temperature_type: 'range', target_temperature_low: 20, target_temperature_high: 22.2 },
            [DN]: { target_temperature_type: 'range', target_temperature_low: 18.9, target_temperature_high: 21.1 }
        }
    };
    // What the accessories hold: pushUpdates builds a request from it (the mode, and the setpoint of a pair that is
    // not being written). Kept as index.js and the accessories keep it: replaced by what HomeKit is shown.
    conn.accessories = { [UP]: { device: Object.assign({}, conn.currentState.shared[UP]) }, [DN]: { device: Object.assign({}, conn.currentState.shared[DN]) } };
    const world = { conn: conn, lines: lines, requests: [], delays: [], shown: [] };
    conn.updateHomeKit = data => {
        for (const id of [UP, DN]) {
            const device = data.devices.thermostats[id];
            Object.assign(conn.accessories[id].device, { target_temperature_type: device.target_temperature_type, target_temperature_low: device.target_temperature_low, target_temperature_high: device.target_temperature_high });
        }
        world.shown.push(showing(conn));
    };
    conn.restarts = 0;
    conn.observeStreamSeq = 1;
    conn.observeRestart = () => conn.restarts++;
    for (let i = 0; i < 500 && !conn.TraitMap; i++) {
        await turn();
    }
    assert.ok(conn.TraitMap, 'protobuf definitions loaded');
    await quiet();
    env.live.clear();
    env.world = world;

    // Answers request number `number` (from 1), `seconds` after it was sent
    const respond = (number, seconds, settle) => {
        const request = world.requests[number - 1];
        assert.ok(request, 'request ' + number + ' was sent');
        const at = request.sent + Math.round(seconds * 1000);
        assert.ok(at >= env.now, 'answers in order of time');
        env.now = at;
        settle(request);
    };
    world.answer = (number, data, seconds) => respond(number, seconds, request => request.resolve({ data: data, status: 200, statusText: 'OK', headers: {}, config: request.config }));
    world.fail = (number, status, seconds) => respond(number, seconds, request => {
        const error = new Error('Request failed with status code ' + status);
        error.response = { status: status, headers: {}, data: Buffer.alloc(0) };
        error.config = request.config;
        request.reject(error);
    });
    world.sent = async count => {
        await until(() => world.requests.length >= count, 'request ' + count);
        await quiet();
        assert.strictEqual(world.requests.length, count, 'requests sent: ' + world.requests.map(request => JSON.stringify(request.carries)).join(' | '));
    };
    // The log without the lines about answers that test/push-answer.test.js covers
    world.said = () => lines.filter(line => !/^info Nest answered the change /.test(line));
    return world;
}

// What a request carries, per trait written: the thermostat, and the mode and pair (or the eco setting)
function carried(conn, config) {
    if (!Buffer.isBuffer(config.data)) {
        return ['REST ' + config.url.replace(/^https:\/\/[^/]+/, '')];
    }
    return decoded(conn, config).map(el => {
        const settings = el.value.settings;
        const name = NAMES[el.id.replace('DEVICE_', '')];
        return settings ? name + ' ' + settings.hvacMode + ' ' + round(settings.targetTemperatureHeat.value) + '/' + round(settings.targetTemperatureCool.value) : name + ' eco ' + el.value.ecoEnabled;
    });
}

function decoded(conn, config) {
    return conn.TraitMap.decode(config.data).set.map(el => {
        const type = conn.proto.root.lookupType(el.property.type_url.split('/')[1]);
        return { id: el.object.id, key: el.object.key, uuid: el.object.uuid, type: el.property.type_url, value: type.toObject(type.decode(el.property.value), { enums: String, defaults: true }) };
    });
}

const round = value => Math.round(value * 10) / 10;

// What HomeKit is shown: 'low/high mode' per thermostat
function showing(conn) {
    const thermostats = conn.apiResponseToObjectTree(conn.mergePendingUpdates(conn.currentState)).devices.thermostats;
    const one = id => round(thermostats[id].target_temperature_low) + '/' + round(thermostats[id].target_temperature_high) + ' ' + thermostats[id].target_temperature_type;
    return { up: one(UP), dn: one(DN) };
}

// The held changes: thermostat, what each writes, whether it is waiting to be pushed or pushed, and for how long it
// still holds
const held = conn => conn.mergeUpdates.map(update => NAMES[update.object.object_key.split('.')[1]] + ' ' + JSON.stringify(update.object.value).replace(/"target_temperature_/g, '"') +
    (update.pushed ? ' pushed ' : ' waiting ') + round((update.expiry_time - env.now) / 1000));

// A HomeKit write, as NestDeviceAccessory.setPropertyAsync makes it: the accessory takes the value, then the change
function set(conn, id, key, value) {
    conn.accessories[id].device[key] = value;
    return conn.update('shared.' + id, key, value, 'range', true);
}

// A HomeKit mode change, as NestThermostatAccessory.setTargetHeatingCooling makes it
function setMode(conn, id, mode) {
    conn.accessories[id].device.target_temperature_type = mode;
    return conn.update('shared.' + id, 'hvac_mode', mode, 'range', true);
}

function writePair(conn, id, low, high) {
    set(conn, id, 'target_temperature_low', low);
    set(conn, id, 'target_temperature_high', high);
}

// A message from Nest reporting these setpoints (or nothing new)
function frame(conn, id, low, high) {
    if (id) {
        Object.assign(conn.currentState.shared[id], { target_temperature_low: low, target_temperature_high: high });
    }
    conn.releaseEchoedMergeUpdates(conn.currentState);
    conn.updateHomeKit(conn.apiResponseToObjectTree(conn.mergePendingUpdates(conn.currentState)));
}

// Timers left running: only the one that ends the hold of the changes still held
function timers(conn) {
    assert.deepStrictEqual(Array.from(env.live), conn.mergeEndTimer ? [conn.mergeEndTimer] : [], 'no timer but the merge timer is left');
    return conn.mergeEndTimer ? 1 : 0;
}

const LOW = 'target_temperature_low', HIGH = 'target_temperature_high';
const DN_PAIR = 'shared.' + DN + ' {"target_temperature_low":18.9,"target_temperature_high":22.8}';

const cases = {
    // The lost write, as it happened, and then Nest applying it when it is sent again
    async 'status 4, then a normal answer: sent twice with the same values, left as a landed write'() {
        const world = await makeWorld();
        const conn = world.conn;
        const start = env.now;
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        assert.deepStrictEqual(world.requests[0].carries, ['dn RANGE 18.9/22.8']);

        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['dn RANGE 18.9/22.8']);
        assert.strictEqual(world.requests[1].sent, start + 15600, 'sent again at once');
        // While it is on its way again: waiting, as when it was made, and HomeKit keeps the written pair
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} waiting 120', 'dn {"high":22.8} waiting 120']);
        assert.deepStrictEqual(showing(conn).dn, '18.9/22.8 range');
        assert.deepStrictEqual(conn.pendingUpdates, []);

        // What differs between the two requests: the uuid of the object written and the time in the update's
        // updateInfo (taken when a request is built), nothing else
        const [first, second] = world.requests.map(request => decoded(conn, request.config));
        assert.notStrictEqual(first[0].uuid, second[0].uuid);
        assert.strictEqual(first[0].value.settings.updateInfo.timestamp.value, Math.floor(start / 1000));
        assert.strictEqual(second[0].value.settings.updateInfo.timestamp.value, Math.floor((start + 15600) / 1000));
        for (const request of [first, second]) {
            request[0].uuid = '';
            request[0].value.settings.updateInfo.timestamp.value = 0;
        }
        assert.deepStrictEqual(second, first);
        const wire = config => ({ method: config.method, url: config.url, timeout: config.timeout, headers: JSON.parse(JSON.stringify(config.headers)), responseType: config.responseType, bytes: config.data.length });
        assert.deepStrictEqual(wire(world.requests[1].config), wire(world.requests[0].config));

        world.answer(2, landed([DN]), 3.3);
        await push;
        // As a landed write is left (test/push-answer.test.js): each setpoint pushed, its 300 s echo wait counted from
        // the answer that was not a status 4, nothing queued, no failure counted
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
        assert.strictEqual(env.now, start + 15600 + 3300);
        assert.deepStrictEqual(conn.pendingUpdates, []);
        assert.strictEqual(conn.failedPushAPICalls, 0);
        assert.strictEqual(world.requests.length, 2);
        assert.deepStrictEqual(world.delays, [0, 0], 'no delay before either request');
        assert.deepStrictEqual(world.lines, [
            // (the first is the line of 2026-10-11 01:31:18Z, but for the device id)
            'info Nest answered the change ' + DN_PAIR + ' after 15.6 s (slow): HTTP 200, 4 byte(s), status 4, starts 12020804.',
            'info Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            'info Nest answered the change ' + DN_PAIR + ' after 3.3 s: HTTP 200, 113 byte(s), operation (progress none, status none), status 0, starts 0a6d0a6b0a5c0a174445564943455f' + Buffer.from(DN).toString('hex') + '12.',
            'info Sent again, the change ' + DN_PAIR + ' was answered after 3.3 s without status 4 (attempt 2 of 2).'
        ]);
        assert.strictEqual(timers(conn), 1);

        // HomeKit was shown the written pair all along; Nest's echo releases it, and nothing is left running
        env.now += 2000;
        frame(conn, DN, 18.9, 22.8);
        assert.deepStrictEqual(held(conn), []);
        assert.ok(world.shown.length > 0 && world.shown.every(shown => shown.dn == '18.9/22.8 range'), JSON.stringify(world.shown));
        assert.strictEqual(timers(conn), 0);
        assert.strictEqual(conn.restarts, 0);
    },

    async 'status 4 every time: two requests, then dropped, said to be lost, and HomeKit shows Nest\'s state'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(showing(conn).dn, '18.9/22.8 range');
        world.answer(2, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 2, 'not sent a third time');
        assert.deepStrictEqual(world.requests.map(request => request.carries), [['dn RANGE 18.9/22.8'], ['dn RANGE 18.9/22.8']]);
        assert.deepStrictEqual(held(conn), [], 'dropped');
        assert.deepStrictEqual(showing(conn).dn, '18.9/21.1 range', 'Nest\'s state');
        assert.deepStrictEqual(world.shown[world.shown.length - 1].dn, '18.9/21.1 range', 'and HomeKit was told so at once');
        assert.ok(world.shown.slice(0, -1).every(shown => shown.dn == '18.9/22.8 range'), 'the written pair until then: ' + JSON.stringify(world.shown));
        assert.deepStrictEqual(conn.accessories[DN].device.target_temperature_high, 21.1);
        assert.deepStrictEqual(conn.pendingUpdates, []);
        assert.strictEqual(conn.failedPushAPICalls, 0);
        assert.deepStrictEqual(world.said(), [
            'info Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            'error Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) when it was sent again (attempt 2 of 2) - these changes were NOT applied: ' + DN_PAIR
        ]);
        assert.strictEqual(timers(conn), 0);
        assert.strictEqual(conn.restarts, 0);

        // The next write starts afresh: sent, and sent again once
        world.lines.length = 0;
        env.now += 30000;
        writePair(conn, DN, 18.9, 22.8);
        const next = conn.pushUpdates();
        await world.sent(3);
        world.answer(3, NOT_APPLIED, 15.6);
        await world.sent(4);
        world.answer(4, landed([DN]), 3.3);
        await next;
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
        clearTimeout(conn.mergeEndTimer);
    },

    // A newer change to the same setting, made while the push was unanswered. Whatever has become of it by the time
    // the status 4 arrives, the older value is not sent again and no longer held.
    async 'a newer change to one setpoint, pushed and answered meanwhile: only the other setpoint is sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 5000;
        set(conn, UP, HIGH, 22.2);
        const newer = conn.pushUpdates();
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up RANGE 21.1/22.2']);
        world.answer(2, landed([UP]), 3);
        await newer;
        // (That push carried the low setpoint too, and settled it: every push settles all changes made before it)
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} pushed 300', 'up {"high":22.2} pushed 300']);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(3);
        // The low setpoint is still this push's, and held: sent again. The request is built with the high setpoint
        // HomeKit is shown, the newer one.
        assert.deepStrictEqual(world.requests[2].carries, ['up RANGE 21.1/22.2']);
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} waiting 120', 'up {"high":22.2} pushed 292.4']);
        assert.deepStrictEqual(world.said(), ['info Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1,"target_temperature_high":22.8} (status 4 after 15.6 s) - sending shared.' + UP + ' {"target_temperature_low":21.1} again (the rest is not: replaced by a newer change, or lost) (attempt 2 of 2).']);
        world.answer(3, landed([UP]), 3);
        await push;
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} pushed 300', 'up {"high":22.2} pushed 289.4']);
        assert.ok(world.requests.every(request => !/22\.8/.test(request.carries.join()) || request === world.requests[0]), '22.8 went out once, before the newer change was made');
        assert.deepStrictEqual(showing(conn).up, '21.1/22.2 range');
        assert.ok(world.shown.every(shown => shown.up == '21.1/22.2 range'), JSON.stringify(world.shown));
        clearTimeout(conn.mergeEndTimer);
    },

    async 'a newer change of the whole pair, pushed and echoed meanwhile: nothing is sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 5000;
        writePair(conn, UP, 20.6, 22.2);
        const newer = conn.pushUpdates();
        await world.sent(2);
        world.answer(2, landed([UP]), 3);
        await newer;
        assert.deepStrictEqual(held(conn), ['up {"low":20.6} pushed 300', 'up {"high":22.2} pushed 300']);
        env.now += 2000;
        frame(conn, UP, 20.6, 22.2);
        assert.deepStrictEqual(held(conn), [], 'echoed: nothing held, and nothing left that says a newer change was made');
        world.answer(1, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 2, 'the older pair is not sent again');
        assert.deepStrictEqual(world.said(), ['info Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1,"target_temperature_high":22.8} (status 4 after 15.6 s) - not sending it again: a newer change has replaced it.']);
        assert.deepStrictEqual(held(conn), []);
        assert.deepStrictEqual(showing(conn).up, '20.6/22.2 range');
        assert.strictEqual(timers(conn), 0);
    },

    async 'a newer change, still queued: it goes out with what is sent again, in place of the older value'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 14000;
        set(conn, UP, HIGH, 22.2); // its debounced push has not started
        assert.deepStrictEqual(showing(conn).up, '21.1/22.2 range');
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up RANGE 21.1/22.2']);
        assert.deepStrictEqual(conn.pendingUpdates, [], 'the queue went out with it');
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} waiting 120', 'up {"high":22.2} waiting 118.4'], 'the older high setpoint is no longer held');
        assert.deepStrictEqual(world.said(), ['info Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1,"target_temperature_high":22.8} (status 4 after 15.6 s) - sending shared.' + UP + ' {"target_temperature_low":21.1} again (the rest is not: replaced by a newer change, or lost) (attempt 2 of 2).']);
        world.answer(2, landed([UP]), 3);
        await push;
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} pushed 300', 'up {"high":22.2} pushed 300']);
        assert.deepStrictEqual(showing(conn).up, '21.1/22.2 range');
        // The queued change's own (debounced) push finds nothing left to send
        assert.strictEqual(await conn.pushUpdates(), null);
        assert.strictEqual(world.requests.length, 2);
        clearTimeout(conn.mergeEndTimer);
    },

    async 'a newer change, pushed and not yet answered: only the newer value goes out again'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 12000;
        set(conn, UP, HIGH, 22.2);
        const newer = conn.pushUpdates();
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up RANGE 21.1/22.2']);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(3);
        // The low setpoint is sent again; the request is built with the high setpoint HomeKit is shown, the newer one
        assert.deepStrictEqual(world.requests[2].carries, ['up RANGE 21.1/22.2']);
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} waiting 120', 'up {"high":22.2} waiting 116.4']);
        world.answer(3, landed([UP]), 3);
        await push;
        world.answer(2, landed([UP]), 8);
        await newer;
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} pushed 298.6', 'up {"high":22.2} pushed 298.6']);
        assert.deepStrictEqual(showing(conn).up, '21.1/22.2 range');
        assert.ok(world.shown.every(shown => shown.up == '21.1/22.2 range'), JSON.stringify(world.shown));
        clearTimeout(conn.mergeEndTimer);
    },

    async 'a newer push that failed has dropped the change meanwhile (as before): not sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 5000;
        set(conn, UP, HIGH, 22.2);
        const newer = conn.pushUpdates();
        await world.sent(2);
        world.fail(2, 400, 3);
        await newer;
        // As before: a failed push drops every change made before it started, so HomeKit shows Nest's state
        assert.deepStrictEqual(held(conn), []);
        assert.deepStrictEqual(showing(conn).up, '20/22.2 range');
        world.lines.length = 0;
        world.answer(1, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 2, 'nothing held, nothing sent again');
        // The high setpoint was replaced by the newer change (lost with its own line); the low one is said to be lost
        assert.deepStrictEqual(world.said(), ['error Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1,"target_temperature_high":22.8} (status 4 after 15.6 s), and it was no longer held (Nest reported another value, or another push that failed dropped it), so it is not sent again - these changes were NOT applied: shared.' + UP + ' {"target_temperature_low":21.1}']);
        assert.strictEqual(timers(conn), 0);
    },

    // Found by the randomized test (write-fuzz.js). The newer change was sent, answered status 4 twice and dropped
    // while the older push was still unanswered: the older value is not sent after it.
    async 'a newer change that was itself given up meanwhile: the older one is not sent again'() {
        for (const settledMeanwhile of [false, true]) {
            const world = await makeWorld();
            const conn = world.conn;
            writePair(conn, DN, 19.4, 22.8);
            const push = conn.pushUpdates();
            await world.sent(1);
            env.now += 1000;
            if (settledMeanwhile) {
                // A push for the other thermostat is answered: that marks the pair pushed (every push settles all
                // changes made before it started), so that it outlives the newer pair being given up
                set(conn, UP, HIGH, 23.3);
                conn.pushUpdates();
                await world.sent(2);
                world.answer(2, landed([UP]), 1);
                await quiet();
            }
            const first = world.requests.length;
            env.now += 2000;
            writePair(conn, DN, 20, 23.3);
            const newer = conn.pushUpdates();
            await world.sent(first + 1);
            world.answer(first + 1, NOT_APPLIED, 1);
            await world.sent(first + 2);
            world.answer(first + 2, NOT_APPLIED, 1);
            await newer;
            // The newer pair is given up, and with it what no push had settled of the older one
            assert.deepStrictEqual(held(conn).filter(line => /^dn/.test(line)).map(line => line.replace(/ [\d.]+$/, '')), settledMeanwhile ? ['dn {"low":19.4} pushed', 'dn {"high":22.8} pushed'] : []);
            assert.deepStrictEqual(showing(conn).dn, settledMeanwhile ? '19.4/22.8 range' : '18.9/21.1 range');
            world.lines.length = 0;
            world.shown.length = 0;
            world.answer(1, NOT_APPLIED, 15.6);
            await push;
            await quiet();
            assert.strictEqual(world.requests.length, first + 2, '19.4/22.8 is not sent after 20/23.3 was');
            assert.deepStrictEqual(world.requests.filter(request => /^dn/.test(request.carries[0])).map(request => request.carries[0]), ['dn RANGE 19.4/22.8', 'dn RANGE 20/23.3', 'dn RANGE 20/23.3']);
            assert.deepStrictEqual(held(conn).filter(line => /^dn/.test(line)), []);
            assert.deepStrictEqual(showing(conn).dn, '18.9/21.1 range');
            // Where the older pair was still held, HomeKit is told Nest's state at once
            assert.deepStrictEqual(world.shown.map(shown => shown.dn), settledMeanwhile ? ['18.9/21.1 range'] : []);
            assert.deepStrictEqual(conn.accessories[DN].device.target_temperature_low, 18.9);
            assert.deepStrictEqual(world.said(), ['info Nest did not apply the change shared.' + DN + ' {"target_temperature_low":19.4,"target_temperature_high":22.8} (status 4 after 15.6 s) - not sending it again: a newer change has replaced it.']);
            clearTimeout(conn.mergeEndTimer);
        }
    },

    // Found by the randomized test. The change went out in another request too (a mode change carries the
    // setpoints), Nest reported it and released it. Its own request is answered status 4: Nest's last report may be
    // older than what Nest holds, so it is sent again all the same.
    async 'no longer held, and Nest last reported this very value: sent again all the same'() {
        const world = await makeWorld();
        const conn = world.conn;
        set(conn, UP, LOW, 21.1); // queued
        setMode(conn, UP, 'heat');
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up HEAT 21.1/22.2']);
        world.answer(1, Buffer.alloc(0), 1);
        world.answer(2, landed([UP]), 1);
        await quiet();
        env.now += 2000;
        conn.currentState.shared[UP].target_temperature_type = 'heat';
        frame(conn, UP, 21.1, 22.2);
        assert.deepStrictEqual(held(conn).filter(line => /^up/.test(line)), []);
        env.now += 5000;
        const push = conn.pushUpdates();
        await world.sent(3);
        world.lines.length = 0;
        world.answer(3, NOT_APPLIED, 15.6);
        await world.sent(4);
        assert.deepStrictEqual(world.requests[3].carries, ['up HEAT 21.1/22.2']);
        world.answer(4, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 4);
        assert.deepStrictEqual(world.said(), [
            'info Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1} (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            'error Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1} (status 4 after 15.6 s) when it was sent again (attempt 2 of 2) - these changes were NOT applied: shared.' + UP + ' {"target_temperature_low":21.1}'
        ]);
        clearTimeout(conn.mergeEndTimer);
    },

    // The count of times sent again stays with the change: sent again, answered 401 and queued for a retry, it goes
    // out with another push before that retry does, and a second status 4 ends it there
    async 'sent again, failed, queued again and taken by another push: still sent again only once'() {
        const world = await makeWorld();
        const conn = world.conn;
        let reauthenticated = null;
        conn.auth = () => new Promise(resolve => {
            reauthenticated = resolve;
        });
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        world.fail(2, 401, 1);
        await until(() => reauthenticated, 'the reauthentication');
        assert.strictEqual(conn.pendingUpdates.length, 1, 'queued for the retry');
        set(conn, UP, HIGH, 23.3);
        const other = conn.pushUpdates(); // (the debounced push of that write)
        await world.sent(3);
        assert.deepStrictEqual(world.requests[2].carries, ['up RANGE 20/23.3', 'dn RANGE 18.9/22.8']);
        world.answer(3, NOT_APPLIED, 15.6);
        await other;
        reauthenticated(true);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 3, 'not sent a fourth time');
        assert.deepStrictEqual(held(conn), []);
        assert.ok(world.said().some(line => /^error .*\(attempt 2 of 2\) - these changes were NOT applied: /.test(line)), world.said().join('\n'));
        assert.strictEqual(timers(conn), 0);
    },

    // Found by the randomized test. A setpoint written just after an eco or mode change is marked pushed when that
    // change's own request is answered, before the setpoint's push has started: it is sent again all the same.
    async 'a setpoint written with a mode change, marked pushed by the mode change\'s answer: sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        setMode(conn, UP, 'heat');
        set(conn, UP, HIGH, 23.3); // queued; the mode change went out at once, in two requests
        await world.sent(2);
        world.answer(1, Buffer.alloc(0), 1);
        world.answer(2, landed([UP]), 1);
        await quiet();
        assert.deepStrictEqual(held(conn).filter(line => /high/.test(line)), ['up {"high":23.3} pushed 300'], 'pushed, though no request has carried it');
        env.now += 6000;
        const push = conn.pushUpdates();
        await world.sent(3);
        world.answer(3, NOT_APPLIED, 15.6);
        await world.sent(4);
        assert.deepStrictEqual(world.requests[3].carries, world.requests[2].carries);
        assert.deepStrictEqual(held(conn).filter(line => /high/.test(line)), ['up {"high":23.3} waiting 120']);
        world.answer(4, NOT_APPLIED, 15.6);
        await push;
        assert.deepStrictEqual(held(conn).filter(line => /high/.test(line)), [], 'given up: dropped');
        assert.ok(/^error .* - these changes were NOT applied: shared\.AAAA000000000001 \{"target_temperature_high":23\.3\}$/.test(world.said()[world.said().length - 1]), world.said().join('\n'));
        clearTimeout(conn.mergeEndTimer);
    },

    // Found by the randomized test. Someone else changes the setpoint while the push is unanswered, and another
    // push's answer has marked the change pushed: Nest's value is shown (as before), and the change is said to be lost.
    async 'released by a change made at Nest meanwhile: not sent again, and said to be lost'() {
        const world = await makeWorld();
        const conn = world.conn;
        set(conn, DN, HIGH, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 3000;
        set(conn, UP, HIGH, 23.3);
        const other = conn.pushUpdates();
        await world.sent(2);
        world.answer(2, landed([UP]), 1);
        await other;
        env.now += 2000;
        frame(conn, DN, 18.9, 20); // not ours, and not what Nest had
        assert.deepStrictEqual(showing(conn).dn, '18.9/20 range');
        world.lines.length = 0;
        world.answer(1, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 2);
        assert.deepStrictEqual(world.said(), ['error Nest did not apply the change shared.' + DN + ' {"target_temperature_high":22.8} (status 4 after 15.6 s), and it was no longer held (Nest reported another value, or another push that failed dropped it), so it is not sent again - these changes were NOT applied: shared.' + DN + ' {"target_temperature_high":22.8}']);
        assert.deepStrictEqual(showing(conn).dn, '18.9/20 range');
        clearTimeout(conn.mergeEndTimer);
    },

    // One push for both thermostats, as on 2026-10-11 00:10:06Z. A bare status 4 is the answer to the whole request.
    async 'two thermostats in one push: both are sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 18.9, 23.3);
        writePair(conn, DN, 18.9, 21.7);
        const push = conn.pushUpdates();
        await world.sent(1);
        // (A request lists the changes last made first; the answer of 00:10:06Z began with the thermostat written last)
        assert.deepStrictEqual(world.requests[0].carries, ['dn RANGE 18.9/21.7', 'up RANGE 18.9/23.3']);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['dn RANGE 18.9/21.7', 'up RANGE 18.9/23.3']);
        assert.strictEqual(LANDED_BOTH.length, 223);
        world.answer(2, LANDED_BOTH, 3.3);
        await push;
        assert.deepStrictEqual(held(conn), ['up {"low":18.9} pushed 300', 'up {"high":23.3} pushed 300', 'dn {"low":18.9} pushed 300', 'dn {"high":21.7} pushed 300']);
        const both = 'shared.' + UP + ' {"target_temperature_low":18.9,"target_temperature_high":23.3}; shared.' + DN + ' {"target_temperature_low":18.9,"target_temperature_high":21.7}';
        assert.deepStrictEqual(world.lines, [
            'info Nest answered the change ' + both + ' after 15.6 s (slow): HTTP 200, 4 byte(s), status 4, starts 12020804.',
            'info Nest did not apply the change ' + both + ' (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            // (the line of 2026-10-11 00:10:06Z, but for the device ids)
            'info Nest answered the change ' + both + ' after 3.3 s: ' + ANSWERED_BOTH,
            'info Sent again, the change ' + both + ' was answered after 3.3 s without status 4 (attempt 2 of 2).'
        ]);
        clearTimeout(conn.mergeEndTimer);
    },

    async 'two thermostats in one push, one written again meanwhile: the other is sent again, with the newer pair'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 18.9, 23.3);
        writePair(conn, DN, 18.9, 21.7);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 14000;
        writePair(conn, UP, 19.4, 22.8);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up RANGE 19.4/22.8', 'dn RANGE 18.9/21.7']);
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} waiting 120', 'dn {"high":21.7} waiting 120', 'up {"low":19.4} waiting 118.4', 'up {"high":22.8} waiting 118.4']);
        world.answer(2, NOT_APPLIED, 15.6);
        await push;
        // Not applied again: all of that request is dropped, the newer pair with it
        assert.deepStrictEqual(held(conn), []);
        assert.deepStrictEqual(showing(conn), { up: '20/22.2 range', dn: '18.9/21.1 range' });
        assert.strictEqual(world.requests.length, 2);
        assert.ok(/^error .* - these changes were NOT applied: shared\.BBBB000000000002 \{"target_temperature_low":18\.9,"target_temperature_high":21\.7\}; shared\.AAAA000000000001 \{"target_temperature_low":19\.4,"target_temperature_high":22\.8\}$/.test(world.said()[1]), world.said()[1]);
        assert.strictEqual(timers(conn), 0);
    },

    // A mode change is pushed at once, in a request of its own, after one that ends eco mode
    async 'a mode change: sent again, alone'() {
        const world = await makeWorld();
        const conn = world.conn;
        setMode(conn, UP, 'heat');
        await world.sent(2);
        assert.deepStrictEqual(world.requests.map(request => request.carries), [['up eco OFF'], ['up HEAT 20/22.2']]);
        set(conn, DN, LOW, 19.4); // queued
        world.answer(1, Buffer.alloc(0), 2);
        await quiet();
        world.answer(2, NOT_APPLIED, 15.6);
        await world.sent(3);
        assert.deepStrictEqual(world.requests[2].carries, ['up HEAT 20/22.2']);
        assert.strictEqual(conn.pendingUpdates.length, 1, 'the queue is left for its own push');
        assert.deepStrictEqual(showing(conn).up, '20/22.2 heat');
        world.answer(3, landed([UP]), 3);
        await quiet();
        assert.deepStrictEqual(held(conn).filter(line => /type/.test(line)), ['up {"target_change_pending":true,"type":"heat"} pushed 300']);
        assert.deepStrictEqual(world.said(), [
            'info Nest did not apply the change shared.' + UP + ' {"target_change_pending":true,"target_temperature_type":"heat"} (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            'info Sent again, the change shared.' + UP + ' {"target_change_pending":true,"target_temperature_type":"heat"} was answered after 3 s without status 4 (attempt 2 of 2).'
        ]);
        clearTimeout(conn.mergeEndTimer);
    },

    async 'an eco change: sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        conn.update('shared.' + UP, 'hvac_mode', 'eco', 'range', true);
        await world.sent(1);
        assert.deepStrictEqual(world.requests[0].carries, ['up eco ON']);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up eco ON']);
        world.answer(2, NOT_APPLIED, 15.6);
        await quiet();
        assert.strictEqual(world.requests.length, 2);
        assert.deepStrictEqual(held(conn), []);
        assert.ok(/^error .* - these changes were NOT applied: device\.AAAA000000000001 \{"eco":\{"mode":"manual-eco"\}\}$/.test(world.said()[1]), world.said()[1]);
        assert.strictEqual(timers(conn), 0);
    },

    async 'a mode change replaced by a newer one: not sent again'() {
        // The newer one answered before the status 4 arrives, and not yet answered
        for (const answeredFirst of [true, false]) {
            const world = await makeWorld();
            const conn = world.conn;
            setMode(conn, UP, 'heat');
            await world.sent(2);
            world.answer(1, Buffer.alloc(0), 2);
            env.now += 5000;
            setMode(conn, UP, 'cool');
            await world.sent(4);
            assert.deepStrictEqual(world.requests[3].carries, ['up COOL 20/22.2']);
            if (answeredFirst) {
                world.answer(3, Buffer.alloc(0), 2);
                world.answer(4, landed([UP]), 3);
                await quiet();
            }
            world.answer(2, NOT_APPLIED, 15.6);
            await quiet();
            assert.strictEqual(world.requests.length, 4, 'heat is not sent again');
            assert.deepStrictEqual(showing(conn).up, '20/22.2 cool');
            assert.deepStrictEqual(held(conn).filter(line => /type/.test(line)).map(line => line.replace(/ (pushed|waiting) .*/, '')), ['up {"target_change_pending":true,"type":"cool"}']);
            assert.ok(/ - not sending it again: a newer change has replaced it\.$/.test(world.said()[0]), world.said()[0]);
            if (!answeredFirst) {
                world.answer(3, Buffer.alloc(0), 12);
                world.answer(4, landed([UP]), 12);
                await quiet();
            }
            assert.strictEqual(world.requests.length, 4);
            clearTimeout(conn.mergeEndTimer);
        }
    },

    async 'a mode change made while a setpoint push was unanswered: what is sent again carries the new mode'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, UP, 21.1, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        env.now += 12000;
        setMode(conn, UP, 'heat');
        await world.sent(3);
        world.delays.length = 0;
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(4);
        assert.deepStrictEqual(world.requests[3].carries, ['up HEAT 21.1/22.8'], 'not put back to RANGE');
        assert.deepStrictEqual(world.delays, [3400], 'as any setpoint push: not within 7 s of a mode change');
        assert.deepStrictEqual(showing(conn).up, '21.1/22.8 heat');
        world.answer(2, Buffer.alloc(0), 6);
        world.answer(3, landed([UP]), 6);
        world.answer(4, landed([UP]), 3);
        await push;
        clearTimeout(conn.mergeEndTimer);
    },

    // Only the exact answer. Each of these is taken as any answer was before: one request, the change pushed and held
    // for its 300 s echo wait.
    async 'other statuses, other bodies: one request, taken as a landed write'() {
        const status = (code, message) => field(2, Buffer.concat([Buffer.from([0x08, code])].concat(message ? [field(2, message)] : [])));
        const bodies = {
            'status 4 with a message': status(4, 'deadline exceeded'),
            'status 4 and another field': Buffer.concat([NOT_APPLIED, Buffer.from([0x18, 0x01])]),
            'status 4 twice': Buffer.concat([NOT_APPLIED, NOT_APPLIED]),
            'a result and status 4': Buffer.concat([landed([DN]).subarray(0, 111), NOT_APPLIED]),
            'a result holding status 4': field(1, Buffer.concat([field(1, 'DEVICE_' + DN), field(3, Buffer.from([0x08, 0x04]))])),
            'status 4 in a longer encoding': Buffer.from('1203088400', 'hex'),
            'status 5': status(5),
            'status 13': status(13),
            'status 14 with a message': status(14, 'unavailable'),
            'status 0': Buffer.from('1200', 'hex'),
            'the landed answer': landed([DN]),
            'empty': Buffer.alloc(0),
            'the first three bytes': NOT_APPLIED.subarray(0, 3),
            'one byte more': Buffer.concat([NOT_APPLIED, Buffer.from([0x00])]),
            'not protobuf': Buffer.from('<html><body>502 Bad Gateway</body></html>'),
            'the four bytes as text': NOT_APPLIED.toString('latin1'),
            'the four bytes as an array': [0x12, 0x02, 0x08, 0x04],
            'no body': undefined
        };
        for (const name in bodies) {
            const world = await makeWorld();
            const conn = world.conn;
            writePair(conn, DN, 18.9, 22.8);
            const push = conn.pushUpdates();
            await world.sent(1);
            world.answer(1, bodies[name], 15.6);
            await push;
            await quiet();
            assert.strictEqual(world.requests.length, 1, name);
            assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300'], name);
            assert.deepStrictEqual(world.said(), [], name);
            assert.deepStrictEqual(conn.pendingUpdates, [], name);
            assert.strictEqual(conn.failedPushAPICalls, 0, name);
            assert.deepStrictEqual(world.shown, [], name);
            clearTimeout(conn.mergeEndTimer);
        }
    },

    // The off switch. With the file there, the answer is taken as before ...
    async 'nest-no-resend.on in the storage directory: one request, taken as a landed write'() {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-no-resend-'));
        try {
            fs.writeFileSync(path.join(dir, 'nest-no-resend.on'), '');
            const world = await makeWorld(dir);
            const conn = world.conn;
            writePair(conn, DN, 18.9, 22.8);
            const push = conn.pushUpdates();
            await world.sent(1);
            world.answer(1, NOT_APPLIED, 15.6);
            await push;
            await quiet();
            assert.strictEqual(world.requests.length, 1);
            assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
            assert.deepStrictEqual(conn.pendingUpdates, []);
            assert.strictEqual(conn.failedPushAPICalls, 0);
            assert.deepStrictEqual(world.shown, []);
            assert.ok(!world.lines.some(line => /^error /.test(line)), world.lines.join('\n'));
            assert.strictEqual(timers(conn), 1);
            clearTimeout(conn.mergeEndTimer);
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    },

    // ... and one line says so. It is looked for at each such answer: no restart to switch it on or off.
    async 'nest-no-resend.on: said in the log, and looked for at each answer'() {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-no-resend-'));
        const flag = path.join(dir, 'nest-no-resend.on');
        try {
            fs.writeFileSync(flag, '');
            const world = await makeWorld(dir);
            const conn = world.conn;
            const lost = async () => {
                writePair(conn, DN, 18.9, 22.8);
                const push = conn.pushUpdates();
                await world.sent(world.requests.length + 1);
                world.answer(world.requests.length, NOT_APPLIED, 15.6);
                return push;
            };
            await lost();
            assert.deepStrictEqual(world.said(), ['info Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) - not sending it again: nest-no-resend.on exists in ' + dir + '.']);
            assert.strictEqual(world.requests.length, 1);

            // Removed: the next one is sent again. Put back while that is unanswered: its status 4 is taken as before.
            fs.unlinkSync(flag);
            world.lines.length = 0;
            env.now += 60000;
            const push = lost();
            await world.sent(3);
            fs.writeFileSync(flag, '');
            world.answer(3, NOT_APPLIED, 15.6);
            await push;
            await quiet();
            assert.strictEqual(world.requests.length, 3);
            assert.deepStrictEqual(world.said(), [
                'info Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
                'info Nest did not apply the change ' + DN_PAIR + ' (status 4 after 15.6 s) - not sending it again: nest-no-resend.on exists in ' + dir + '.'
            ]);
            assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
            clearTimeout(conn.mergeEndTimer);
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    },

    // The slowest answers there can be (a request is given up after 40 s), after the longest debounce: the hold of a
    // change waiting to be pushed (120 s) does not run out while it is sent again
    async 'slow answers: the written values stay held until Nest has answered'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, DN, 18.9, 22.8);
        env.now += 8000;
        const push = conn.pushUpdates();
        await world.sent(1);
        const check = () => {
            conn.scheduleMergeEnd(); // what the merge timer does when it fires
            assert.deepStrictEqual(showing(conn).dn, '18.9/22.8 range');
            assert.deepStrictEqual(world.shown.filter(shown => shown.dn != '18.9/22.8 range'), []);
        };
        world.answer(1, NOT_APPLIED, 39.9);
        await world.sent(2);
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} waiting 120', 'dn {"high":22.8} waiting 120']);
        for (let seconds = 0; seconds < 39; seconds += 13) {
            env.now += 13000;
            check();
        }
        world.answer(2, landed([DN]), 39.9);
        await push;
        check();
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
        // The echo wait is counted from the second answer: 299 s later the pair is still held, 2 s after that Nest's
        // current state is read (echo-refresh.test.js)
        env.now += 299000;
        check();
        assert.strictEqual(conn.restarts, 0);
        env.now += 2000;
        conn.scheduleMergeEnd();
        await quiet();
        assert.strictEqual(conn.restarts, 1);
        assert.ok(world.lines.some(line => /^info Nest has not echoed /.test(line)));
        clearTimeout(conn.mergeEndTimer);
    },

    // What is sent again can fail as any push can, and is retried as any push is; the count of times sent again is
    // kept through that
    async 'sent again and answered 401: retried after reauthentication, then a status 4 ends it'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        world.fail(2, 401, 1);
        await world.sent(3);
        assert.ok(world.lines.includes('reauthentication'));
        assert.deepStrictEqual(world.requests[2].carries, ['dn RANGE 18.9/22.8']);
        assert.deepStrictEqual(showing(conn).dn, '18.9/22.8 range');
        world.answer(3, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 3);
        assert.deepStrictEqual(held(conn), []);
        assert.ok(world.said().some(line => /^error .*\(attempt 2 of 2\) - these changes were NOT applied/.test(line)), world.said().join('\n'));
        assert.strictEqual(timers(conn), 0);
    },

    async 'sent again and answered 500: dropped and said to be lost, as any failed push'() {
        const world = await makeWorld();
        const conn = world.conn;
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        world.fail(2, 500, 1);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 2);
        assert.deepStrictEqual(held(conn), []);
        assert.deepStrictEqual(showing(conn).dn, '18.9/21.1 range');
        assert.ok(world.said().some(line => /^error Nest API call to change device settings returned an error: HTTP 500 .* - these changes were NOT applied: shared\.BBBB000000000002 /.test(line)), world.said().join('\n'));
        assert.strictEqual(conn.failedPushAPICalls, 1);
        assert.strictEqual(timers(conn), 0);
    },

    // A change for the REST API in the same push went out, and was answered, before the protobuf request
    async 'a REST change in the same push is not sent again'() {
        const world = await makeWorld();
        const conn = world.conn;
        conn.commitUpdate('device.T9', { fan_timer_timeout: 0 }, null, false);
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, {}, 1);
        await world.sent(2);
        world.answer(2, NOT_APPLIED, 15.6);
        await world.sent(3);
        world.answer(3, landed([DN]), 3);
        await push;
        assert.deepStrictEqual(world.requests.map(request => request.carries), [['REST /v5/put'], ['dn RANGE 18.9/22.8'], ['dn RANGE 18.9/22.8']]);
        assert.deepStrictEqual(conn.mergeUpdates.map(update => update.object.object_key.split('.')[1] + (update.pushed ? ' pushed' : ' waiting')), ['T9 pushed', DN + ' pushed', DN + ' pushed']);
        clearTimeout(conn.mergeEndTimer);
    },

    // An earlier pushed change to the same setting, still waiting for its echo, is left as it was
    async 'given up: an earlier change still waiting for its echo is left to its own wait'() {
        const world = await makeWorld();
        const conn = world.conn;
        set(conn, DN, HIGH, 21.7);
        const earlier = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, landed([DN]), 3);
        await earlier;
        env.now += 60000;
        set(conn, DN, HIGH, 22.8);
        const push = conn.pushUpdates();
        await world.sent(2);
        world.answer(2, NOT_APPLIED, 15.6);
        await world.sent(3);
        assert.deepStrictEqual(held(conn), ['dn {"high":21.7} pushed 224.4', 'dn {"high":22.8} waiting 120']);
        assert.deepStrictEqual(showing(conn).dn, '18.9/22.8 range');
        world.answer(3, NOT_APPLIED, 15.6);
        await push;
        assert.deepStrictEqual(held(conn), ['dn {"high":21.7} pushed 208.8']);
        assert.deepStrictEqual(showing(conn).dn, '18.9/21.7 range');
        clearTimeout(conn.mergeEndTimer);
    },

    // In a single mode a threshold written by HomeKit goes out as the target temperature (pushUpdates, the Siri
    // workaround), under another key than the change was made with: sent again the same way
    async 'heat mode, the heating threshold written: sent again as it was sent'() {
        const world = await makeWorld();
        const conn = world.conn;
        conn.currentState.shared[UP].target_temperature_type = 'heat';
        conn.accessories[UP].device.target_temperature_type = 'heat';
        conn.accessories[UP].device.target_temperature_low = 21.1;
        conn.update('shared.' + UP, LOW, 21.1, 'heat', true);
        const push = conn.pushUpdates();
        await world.sent(1);
        assert.deepStrictEqual(world.requests[0].carries, ['up HEAT 21.1/22.2']);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        assert.deepStrictEqual(world.requests[1].carries, ['up HEAT 21.1/22.2']);
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} waiting 120']);
        world.answer(2, landed([UP]), 3);
        await push;
        assert.deepStrictEqual(held(conn), ['up {"low":21.1} pushed 300']);
        assert.deepStrictEqual(world.said(), [
            'info Nest did not apply the change shared.' + UP + ' {"target_temperature_low":21.1} (status 4 after 15.6 s) - sending it again (attempt 2 of 2).',
            'info Sent again, the change shared.' + UP + ' {"target_temperature":21.1} was answered after 3 s without status 4 (attempt 2 of 2).'
        ]);
        clearTimeout(conn.mergeEndTimer);
    },

    // Anything going wrong while it is decided what to send again: the answer is taken as before
    async 'deciding what to send again fails: one request, taken as a landed write'() {
        const world = await makeWorld();
        const conn = world.conn;
        conn.resendNotApplied = () => {
            throw new Error('failed');
        };
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await push;
        await quiet();
        assert.strictEqual(world.requests.length, 1);
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
        assert.deepStrictEqual(world.said(), []);
        assert.strictEqual(conn.failedPushAPICalls, 0);
        clearTimeout(conn.mergeEndTimer);
    },

    // A logger that throws changes nothing (as for the line about the answer, test/push-answer.test.js)
    async 'a logger that throws: sent again all the same'() {
        const world = await makeWorld();
        const conn = world.conn;
        conn.log = line => {
            if (/did not apply|Sent again/.test(String(line))) {
                throw new Error('logger failed');
            }
        };
        writePair(conn, DN, 18.9, 22.8);
        const push = conn.pushUpdates();
        await world.sent(1);
        world.answer(1, NOT_APPLIED, 15.6);
        await world.sent(2);
        world.answer(2, landed([DN]), 3.3);
        await push;
        assert.deepStrictEqual(held(conn), ['dn {"low":18.9} pushed 300', 'dn {"high":22.8} pushed 300']);
        assert.deepStrictEqual(world.lines.filter(line => /^error /.test(line)), []);
        clearTimeout(conn.mergeEndTimer);
    }
};

async function withEnvironment(fn) {
    const real = { adapter: axios.defaults.adapter, now: Date.now, delay: Promise.delay };
    env.now = real.now();
    Date.now = () => env.now;
    // Requests wait for the case to answer them
    axios.defaults.adapter = config => new Promise((resolve, reject) => {
        const world = env.world;
        world.requests.push({ config: config, sent: env.now, carries: carried(world.conn, config), resolve: resolve, reject: reject });
    });
    // The delay before a push is sent is noted, not waited for
    Promise.delay = ms => {
        env.world.delays.push(ms);
        return new Promise(resolve => realSetTimeout(resolve, 0));
    };
    global.setTimeout = (fn, ms, ...args) => {
        const handle = realSetTimeout(() => {
            env.live.delete(handle);
            fn(...args);
        }, ms);
        env.live.add(handle);
        return handle;
    };
    global.clearTimeout = handle => {
        env.live.delete(handle);
        realClearTimeout(handle);
    };
    try {
        return await fn();
    } finally {
        axios.defaults.adapter = real.adapter;
        Date.now = real.now;
        Promise.delay = real.delay;
        global.setTimeout = realSetTimeout;
        global.clearTimeout = realClearTimeout;
    }
}

module.exports = async function() {
    await withEnvironment(async () => {
        for (const name in cases) {
            try {
                await cases[name]();
            } catch (error) {
                error.message = name + ': ' + error.message;
                throw error;
            }
        }
    });
};

// Each case on its own (for running them against another version of the plugin)
module.exports.cases = Object.keys(cases).reduce((all, name) => Object.assign(all, { [name]: () => withEnvironment(cases[name]) }), {});
