'use strict';

const assert = require('assert');
const { hap, makeThermostat, makeSensor, readWire, thermostatDevice } = require('./harness');
const Connection = require('../lib/nest-connection');
const { Service, Characteristic } = hap;

const silent = { info() {}, debug() {}, error() {}, warn() {} };
const FAILURE = hap.HAPStatus.SERVICE_COMMUNICATION_FAILURE;

async function rejects(promise) {
    try {
        await promise;
    } catch (status) {
        return status;
    }
    assert.fail('expected a rejection');
}

module.exports = async function() {
    const realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    try {
        const conn = new Connection({}, silent, false, false);
        conn.connected = true;
        conn.token = 'token';
        const pushed = [];
        conn.update = (...args) => { pushed.push(args); return Promise.resolve(true); };

        const t = makeThermostat({}, [], conn);
        const s = makeSensor({}, [], conn);
        const svc = t.getService(Service.Thermostat);
        const sensorSvc = s.getService(Service.TemperatureSensor);

        // Fresh: reads and writes work
        now += 9 * 60 * 1000;
        assert.ok(Math.abs(await readWire(svc, Characteristic.CurrentTemperature) - 20.37) < 1e-9);
        await svc.getCharacteristic(Characteristic.HeatingThresholdTemperature).handleSetRequest(20);
        assert.strictEqual(pushed.length, 1);

        // Stale (default 10 min without contact): reads fault, writes are refused before touching state
        now += 2 * 60 * 1000;
        assert.ok(conn.isStale());
        assert.strictEqual(await rejects(readWire(svc, Characteristic.CurrentTemperature)), FAILURE);
        assert.strictEqual(await rejects(readWire(sensorSvc, Characteristic.CurrentTemperature)), FAILURE);
        assert.strictEqual(await rejects(readWire(svc, Characteristic.CurrentHeatingCoolingState)), FAILURE);
        assert.strictEqual(await rejects(svc.getCharacteristic(Characteristic.TargetHeatingCoolingState).handleSetRequest(Characteristic.TargetHeatingCoolingState.HEAT)), FAILURE);
        assert.strictEqual(t.device.target_temperature_type, 'range', 'refused write left local state alone');
        assert.strictEqual(pushed.length, 1);

        // No events while stale (an event would make the controller think the value is healthy again)
        const before = svc.getCharacteristic(Characteristic.CurrentTemperature).value;
        t.updateData(thermostatDevice({ current_temperature: 25 }));
        assert.strictEqual(svc.getCharacteristic(Characteristic.CurrentTemperature).value, before);

        // Contact restores everything
        conn.noteContact();
        assert.ok(!conn.isStale());
        assert.ok(Math.abs(await readWire(svc, Characteristic.CurrentTemperature) - 25) < 1e-9);

        // Disconnected with no reauthentication under way: writes refused; while one is under way, accepted
        conn.connected = false;
        assert.strictEqual(conn.writeBlockedReason(), 'not connected to Nest');
        assert.strictEqual(await rejects(svc.getCharacteristic(Characteristic.CoolingThresholdTemperature).handleSetRequest(23)), FAILURE);
        conn.authPromise = new Promise(() => {});
        assert.strictEqual(conn.writeBlockedReason(), null);
        conn.authPromise = null;

        // Observe stream: data and keep-alive messages are contact, error statuses are not
        {
            const c = new Connection({}, silent, false, false);
            for (let i = 0; i < 200 && !c.StreamBody; i++) {
                await new Promise(resolve => setTimeout(resolve, 10));
            }
            const encode = obj => Buffer.from(c.StreamBody.encode(c.StreamBody.fromObject(obj)).finish());
            const start = Date.now();
            now += 60000;
            c.decodeObserveMessage(encode({ status: { code: 13, message: 'internal' } }));
            assert.strictEqual(c.lastContactTime, start, 'error status is not contact');
            c.decodeObserveMessage(Buffer.from([0xff, 0xff, 0xff]));
            assert.strictEqual(c.lastContactTime, start, 'undecodable message is not contact');
            c.decodeObserveMessage(encode({ noop: [Buffer.from('x')] }));
            assert.strictEqual(c.lastContactTime, now, 'keep-alive is contact');
        }

        // staleDataTimeoutMinutes: 0 disables
        const never = new Connection({ staleDataTimeoutMinutes: 0 }, silent, false, false);
        now += 24 * 60 * 60 * 1000;
        assert.ok(!never.isStale());
        const custom = new Connection({ staleDataTimeoutMinutes: 30 }, silent, false, false);
        now += 29 * 60 * 1000;
        assert.ok(!custom.isStale());
        now += 2 * 60 * 1000;
        assert.ok(custom.isStale());
    } finally {
        Date.now = realNow;
    }
};
