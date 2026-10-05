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

    // A written value keeps forcing until shortly after its push completes, however long the push is delayed
    withClock(clock => {
        const conn = makeConnection();
        const t0 = clock.now;
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        clock.advance(30000); // debounce + mode-change delay + slow API call; upstream expired after 8 s
        assert.strictEqual(forcedLow(conn), 21, 'still forced before the push completed');
        conn.settleMergeUpdates(t0 + 29000, true);
        clock.advance(7000);
        assert.strictEqual(forcedLow(conn), 21, 'forced shortly after the push');
        clock.advance(2000);
        assert.strictEqual(forcedLow(conn), 20, 'expired 8 s after the push');
        conn.scheduleMergeEnd();
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
