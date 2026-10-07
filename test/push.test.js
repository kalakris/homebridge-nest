'use strict';

const assert = require('assert');
const { hap, makeThermostat, makeSensor, thermostatDevice } = require('./harness');
const Connection = require('../lib/nest-connection');
const { Service, Characteristic } = hap;

const silent = { info() {}, debug() {}, error() {}, warn() {} };

function withClock(fn) {
    const realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    const clock = { advance(ms) { now += ms; }, get now() { return now; } };
    try {
        return fn(clock);
    } finally {
        Date.now = realNow;
    }
}

function makeConnection() {
    const conn = new Connection({}, silent, false, false);
    conn.pushUpdatesDebounced = () => {};
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.T1'] } },
        where: { S1: { wheres: [] } },
        device: { T1: { target_temperature_type: 'range', can_heat: true, can_cool: true } },
        shared: { T1: { target_temperature_type: 'range', target_temperature_low: 20, target_temperature_high: 22 } }
    };
    conn.resyncs = [];
    conn.updateHomeKit = data => conn.resyncs.push(data.devices.thermostats.T1.target_temperature_low);
    return conn;
}

const forcedLow = conn => conn.apiResponseToObjectTree(conn.mergePendingUpdates(conn.currentState)).devices.thermostats.T1.target_temperature_low;

module.exports = async function() {
    // Characteristic values are pushed (updateValue) when new data arrives, not only when polled
    {
        const t = makeThermostat({ current_temperature: 20.37 });
        const characteristic = t.getService(Service.Thermostat).getCharacteristic(Characteristic.CurrentTemperature);
        const events = [];
        characteristic.on('change', change => events.push(change.newValue));
        t.updateData(thermostatDevice({ current_temperature: 20.52, hvac_state: 'heating' }));
        assert.ok(Math.abs(characteristic.value - 20.52) < 1e-9, 'pushed value ' + characteristic.value);
        assert.deepStrictEqual(events.length, 1);
        assert.strictEqual(t.getService(Service.Thermostat).getCharacteristic(Characteristic.CurrentHeatingCoolingState).value, Characteristic.CurrentHeatingCoolingState.HEAT);

        const s = makeSensor({ current_temperature: 20 });
        s.updateData(Object.assign({}, s.device, { current_temperature: 21.04 }));
        assert.ok(Math.abs(s.getService(Service.TemperatureSensor).getCharacteristic(Characteristic.CurrentTemperature).value - 21.04) < 1e-9);
    }

    // A written value keeps forcing until Nest echoes it, however long the push or the echo takes
    withClock(clock => {
        const conn = makeConnection();
        const t0 = clock.now;
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        clock.advance(30000); // debounce + mode-change delay + slow API call
        assert.strictEqual(forcedLow(conn), 21, 'still forced before the push completed');
        conn.settleMergeUpdates(t0 + 29000, true);
        clock.advance(120000); // 2026-10-06: Nest echoed a write ~2 min later; upstream re-synced the old value after 8 s
        conn.releaseEchoedMergeUpdates(conn.currentState); // unrelated data: the setting still reads the old value
        assert.strictEqual(forcedLow(conn), 21, 'still forced while Nest has not echoed');
        assert.deepStrictEqual(conn.resyncs, [], 'no re-sync to the pre-write value');
        conn.currentState.shared.T1.target_temperature_low = Math.fround(21.0004); // the echo, float32-rounded
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 0, 'released by the echo');
        assert.ok(Math.abs(forcedLow(conn) - 21) < 0.01);
        assert.strictEqual(conn.mergeEndTimer, null);
    });

    // Another change to the same setting after ours (app, schedule) releases ours: Nest's value wins
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        conn.currentState.shared.T1.target_temperature_low = 19.5;
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(forcedLow(conn), 19.5);
    });

    // Two writes within the echo delay (re-send, hand-back): the echo of the first must not release the second
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(60000);
        conn.commitUpdate('shared.T1', { target_temperature_low: 20.5 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        assert.strictEqual(forcedLow(conn), 20.5);
        conn.currentState.shared.T1.target_temperature_low = 21; // echo of the FIRST write
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(forcedLow(conn), 20.5, 'the second write still holds');
        conn.currentState.shared.T1.target_temperature_low = 20.5; // echo of the second
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 0);
        assert.strictEqual(forcedLow(conn), 20.5);
    });

    // A second write while Nest still reports the value before the first (re-send, block edge): a frame for the
    // device that echoes neither must not release the second write as "someone else's change"
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(10000);
        conn.commitUpdate('shared.T1', { target_temperature_low: 21.5 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        conn.releaseEchoedMergeUpdates(conn.currentState); // Nest still reports 20
        assert.strictEqual(forcedLow(conn), 21.5, 'the second write still holds');
        conn.currentState.shared.T1.target_temperature_low = 21; // echo of the first
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(forcedLow(conn), 21.5);
        conn.currentState.shared.T1.target_temperature_low = 21.5;
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 0);
        clearTimeout(conn.mergeEndTimer);
    });

    // Nest coalesces: only the second write is echoed -> both released, the echoed value shows
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        conn.commitUpdate('shared.T1', { target_temperature_low: 20.5 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        conn.currentState.shared.T1.target_temperature_low = 20.5;
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 0);
        assert.strictEqual(forcedLow(conn), 20.5);
    });

    // A pushed change that is never echoed is dropped after API_MERGE_ECHO_MAX_SECONDS, and HomeKit re-synced, when no
    // fresh read is possible (not connected; see echo-refresh.test.js for the refresh)
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(299000);
        assert.strictEqual(forcedLow(conn), 21);
        clock.advance(2000);
        conn.scheduleMergeEnd();
        assert.strictEqual(forcedLow(conn), 20);
        assert.deepStrictEqual(conn.resyncs, [20], 're-synced once on expiry');
        assert.strictEqual(conn.mergeEndTimer, null);
    });

    // A failed push drops the forced value at once, so HomeKit sees the real state
    withClock(clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        clock.advance(3000);
        conn.settleMergeUpdates(clock.now, false);
        assert.deepStrictEqual(conn.resyncs, [20]);
        assert.strictEqual(forcedLow(conn), 20);
        assert.strictEqual(conn.mergeEndTimer, null);
    });

    // A change committed after a push started is not settled by it
    withClock(clock => {
        const conn = makeConnection();
        const pushStart = clock.now;
        clock.advance(1000);
        conn.commitUpdate('shared.T1', { target_temperature_low: 21.5 }, 'range', true);
        conn.settleMergeUpdates(pushStart, false);
        assert.strictEqual(forcedLow(conn), 21.5);
        clearTimeout(conn.mergeEndTimer);
    });
};
