'use strict';

// The start-up: the platform builds its accessories once, from Nest's device list, and replaces every accessory
// Homebridge restored from its cache by what it built. 2026-10-10 18:05Z: the first observe stream after a restart
// ended without the device list, the start went on without it, and both thermostats and the three temperature sensors
// were removed from HomeKit until the next restart. A start now waits for the list, whatever the first streams do.
//
// Driven through the real index.js, Connection.observe / protobufDataTimerLoop / updateProtobufData and the real
// protobuf decoding: only http2.connect (the observe stream), the REST subscribe call and authentication are replaced.

const assert = require('assert');
const EventEmitter = require('events');
const fs = require('fs');
const http2 = require('http2');
const os = require('os');
const path = require('path');
const varint = require('varint');
// The harness initialises the accessory classes (with its PlatformAccessory stand-in) before index.js is asked to
const { hap } = require('./harness');
const registerPlugin = require('../index.js');
const Connection = require('../lib/nest-connection');

const { Service, Characteristic } = hap;
const FAILURE = hap.HAPStatus.SERVICE_COMMUNICATION_FAILURE;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function until(condition, what) {
    for (let i = 0; i < 400; i++) {
        if (condition()) {
            return;
        }
        await sleep(5);
    }
    assert.fail('timed out waiting for ' + what);
}

async function rejects(promise) {
    try {
        await promise;
    } catch (status) {
        return status;
    }
    assert.fail('expected a rejection');
}

let NestPlatform;
registerPlugin({
    platformAccessory: function() {},
    hap: hap,
    registerPlatform: (plugin, name, platform) => {
        NestPlatform = platform;
    }
});

// ---------------------------------------------------------------- what Nest sends

const THERMOSTAT = 'nest.resource.NestOnyxResource', SENSOR = 'nest.resource.NestKryptoniteResource';
const HOUSE = [
    { id: 'T1', type: THERMOSTAT, where: 'W1', sensors: ['K1', 'K2'] },
    { id: 'T2', type: THERMOSTAT, where: 'W2', sensors: ['K3'] },
    { id: 'K1', type: SENSOR, where: 'W3' },
    { id: 'K2', type: SENSOR, where: 'W4' },
    { id: 'K3', type: SENSOR, where: 'W5' },
    { id: 'C1', type: 'google.resource.NeonQuartzResource', where: 'W1' }
];
const WHERES = { W1: 'Upstairs', W2: 'Downstairs', W3: 'Nursery', W4: 'Master Bedroom', W5: 'Living Room' };
const NAMES = ['Downstairs Thermostat', 'Home Occupied', 'Living Room', 'Master Bedroom', 'Nursery', 'Upstairs Thermostat'];

// One length-delimited protobuf field
const field = (number, payload) => Buffer.concat([Buffer.from(varint.encode((number << 3) | 2)), Buffer.from(varint.encode(payload.length)), Buffer.from(payload)]);

// The first message of every stream (41 kB here): the resources and their trait types, no values. The plugin's schema
// does not describe it; it must do nothing.
function resourcesFrame(devices) {
    const entries = devices.map(device => field(1, Buffer.concat([
        field(1, 'DEVICE_' + device.id),
        field(2, device.type),
        field(4, Buffer.concat([field(1, 'target_temperature_settings'), field(2, 'nest.trait.hvac.TargetTemperatureSettingsTrait')]))
    ])));
    return field(1, Buffer.concat(entries));
}

// The second message: every trait's current value (the device list among them: peer_devices)
function listFrame(conn, devices) {
    const root = conn.proto.root;
    const get = (id, key, type, value) => {
        const Type = root.lookupType(type);
        return { object: { id: id, key: key }, data: { property: { type_url: 'type.nestlabs.com/' + type, value: Type.encode(Type.fromObject(value)).finish() } } };
    };
    const gets = [
        get('USER_U1', 'user_info', 'nest.trait.user.UserInfoTrait', { legacyId: 'user.1' }),
        get('STRUCTURE_SX', 'structure_info', 'nest.trait.structure.StructureInfoTrait', { legacyId: 'structure.S1' }),
        get('STRUCTURE_SX', 'located_annotations', 'nest.trait.located.LocatedAnnotationsTrait',
            { annotations: Object.keys(WHERES).map(id => ({ info: { id: { value: id }, name: { value: WHERES[id] } } })) }),
        get('STRUCTURE_SX', 'peer_devices', 'weave.trait.peerdevices.PeerDevicesTrait',
            { devices: devices.map(device => ({ data: { deviceId: { value: 'DEVICE_' + device.id }, deviceType: { value: device.type }, fwVersion: '1.0' } })) })
    ];
    devices.forEach(device => {
        const id = 'DEVICE_' + device.id;
        gets.push(get(id, 'liveness', 'weave.trait.heartbeat.LivenessTrait', { status: 'LIVENESS_DEVICE_STATUS_ONLINE' }));
        gets.push(get(id, 'device_located_settings', 'nest.trait.located.DeviceLocatedSettingsTrait', { whereId: { value: device.where } }));
        gets.push(get(id, 'device_identity', 'weave.trait.description.DeviceIdentityTrait', { modelName: { value: 'Model' }, serialNumber: 'SERIAL' + device.id, fwVersion: '1.0' }));
        gets.push(get(id, 'current_temperature', 'nest.trait.sensor.TemperatureTrait', { temperature: { value: { value: 21.5 } } }));
        if (device.type == THERMOSTAT) {
            gets.push(get(id, 'hvac_equipment_capabilities', 'nest.trait.hvac.HvacEquipmentCapabilitiesTrait', { canCool: 1, canHeat: 1 }));
            gets.push(get(id, 'hvac_control', 'nest.trait.hvac.HvacControlTrait', { settings: { isCooling: 0, isHeating: 0 } }));
            gets.push(get(id, 'target_temperature_settings', 'nest.trait.hvac.TargetTemperatureSettingsTrait',
                { settings: { hvacMode: 'RANGE', targetTemperatureHeat: { value: 20 }, targetTemperatureCool: { value: 22 } }, active: { value: 1 } }));
            gets.push(get(id, 'display_settings', 'nest.trait.hvac.DisplaySettingsTrait', { units: 'DEGREES_F' }));
            gets.push(get(id, 'remote_comfort_sensing_settings', 'nest.trait.hvac.RemoteComfortSensingSettingsTrait',
                { associatedRcsSensors: device.sensors.filter(sensor => devices.some(el => el.id == sensor)).map(sensor => ({ deviceId: { resourceId: 'DEVICE_' + sensor } })) }));
            gets.push(get(id, 'backplate_temperature', 'nest.trait.sensor.TemperatureTrait', { temperature: { value: { value: 21 } } }));
            gets.push(get(id, 'current_humidity', 'nest.trait.sensor.HumidityTrait', { humidity: { value: { value: 50 } } }));
        }
    });
    return Buffer.from(conn.StreamBody.encode(conn.StreamBody.fromObject({ message: [{ get: gets }] })).finish());
}

const statusFrame = (conn, code) => Buffer.from(conn.StreamBody.encode(conn.StreamBody.fromObject({ status: { code: code } })).finish());

// ---------------------------------------------------------------- one start

// A cached accessory as Homebridge restores it: services and stored values, nothing handling them
function cachedAccessory(name, deviceType, deviceId) {
    const uuid = hap.uuid.generate('nest.' + deviceType + '.' + deviceId);
    const accessory = new hap.Accessory(name, uuid);
    if (deviceType == 'thermostat') {
        const service = accessory.addService(Service.Thermostat, name);
        service.getCharacteristic(Characteristic.CurrentTemperature).updateValue(19.5);
        service.getCharacteristic(Characteristic.HeatingThresholdTemperature).updateValue(18);
    }
    return { displayName: name, UUID: uuid, services: accessory.services, hapAccessory: accessory };
}

async function start(cached) {
    const lines = [], calls = [];
    const text = args => args.map(el => typeof el == 'string' ? el : JSON.stringify(el)).join(' ');
    const log = Object.assign((...args) => lines.push('info ' + text(args)), {
        info: (...args) => lines.push('info ' + text(args)),
        warn: (...args) => lines.push('warn ' + text(args)),
        error: (...args) => lines.push('error ' + text(args)),
        debug: () => {}
    });
    const api = new EventEmitter();
    api.user = { storagePath: () => path.join(os.tmpdir(), 'homebridge-nest-test-no-such-dir') };
    api.registered = [];
    api.unregisterPlatformAccessories = (plugin, platform, accessories) => {
        calls.push('unregister ' + accessories.map(el => el.displayName).sort().join(', '));
        api.registered = api.registered.filter(el => !accessories.includes(el));
    };
    api.registerPlatformAccessories = (plugin, platform, accessories) => {
        calls.push('register ' + accessories.map(el => el.displayName).sort().join(', '));
        api.registered = api.registered.concat(accessories);
    };

    const platform = new NestPlatform(log, { options: [] }, api);
    cached = cached || [
        cachedAccessory('Upstairs Thermostat', 'thermostat', 'T1'), cachedAccessory('Downstairs Thermostat', 'thermostat', 'T2'),
        cachedAccessory('Nursery', 'temp_sensor', 'K1'), cachedAccessory('Master Bedroom', 'temp_sensor', 'K2'),
        cachedAccessory('Living Room', 'temp_sensor', 'K3'), cachedAccessory('Home Occupied', 'home_away_sensor', 'S1')
    ];
    cached.forEach(accessory => {
        platform.configureAccessory(accessory);
        api.registered.push(accessory);
    });

    const conn = new Connection({}, log, false, false, api.user.storagePath());
    conn.connected = true;
    conn.token = 'token';
    conn.authAttempt = async () => true;
    // The REST API: the structure (Home Occupied), then nothing more
    let subscribes = 0;
    conn.updateData = () => {
        if (subscribes++) {
            return new Promise(() => {});
        }
        conn.currentState = { structure: { S1: { structure_id: 'S1', name: 'Home', away: false, swarm: [] } }, where: { S1: { wheres: [] } } };
        return Promise.resolve(conn.apiResponseToObjectTree(conn.currentState));
    };
    platform.setupConnection = async () => conn;
    await until(() => conn.StreamBody, 'the protobuf schema');

    // The observe stream: each http2.connect is one stream, handed to the test to script
    const streams = [];
    http2.connect = () => {
        const client = new EventEmitter();
        const request = new EventEmitter();
        client.destroy = () => {};
        client.ping = () => {};
        client.request = () => request;
        request.write = () => {};
        request.destroy = () => {};
        request.end = () => streams.push({
            send: frame => request.emit('data', frame),
            close: () => client.emit('close'),
            fail: error => request.emit('error', error)
        });
        return client;
    };
    let opened = 0;
    const nextStream = async () => {
        await until(() => streams.length > opened, 'observe stream ' + (opened + 1));
        return streams[opened++];
    };

    api.emit('didFinishLaunching');
    return {
        platform, api, conn, lines, calls, cached, nextStream,
        names: () => api.registered.map(el => el.displayName).sort(),
        // Leaves both data loops parked, with no timers running
        async stop() {
            conn.ensureConnected = () => new Promise(() => {});
            if (opened) {
                streams[opened - 1].close();
            }
            await sleep(20);
        }
    };
}

const noticeLines = lines => lines.filter(line => /Still waiting|ended the device stream|Device list received/.test(line));

module.exports = async function() {
    const realConnect = http2.connect, realDelay = Promise.delay;
    // The loops' own waits (0.1 s between streams, 10 s after an error) are not what is tested
    Promise.delay = ms => realDelay(Math.min(ms, 5));
    try {
        // A normal start: the list arrives with the first stream's second message
        {
            const s = await start();
            const stream = await s.nextStream();
            stream.send(resourcesFrame(HOUSE));
            await sleep(10);
            assert.deepStrictEqual(s.calls, [], 'the first message alone does nothing');
            stream.send(listFrame(s.conn, HOUSE));
            await until(() => s.calls.length == 2, 'the accessories');
            assert.deepStrictEqual(s.calls, ['unregister ' + NAMES.join(', '), 'register ' + NAMES.join(', ')]);
            assert.deepStrictEqual(noticeLines(s.lines), [], 'nothing new in the log');
            assert.deepStrictEqual(s.lines.map(line => line.replace(/:? deviceId.*/, '')), [
                'info Fetching Nest devices.',
                'info UNSUPPORTED DEVICE TYPE DETECTED: google.resource.NeonQuartzResource (Device ID: C1) - Please report this',
                'info initing thermostat (P) "Upstairs Thermostat"',
                'info initing thermostat (P) "Downstairs Thermostat"',
                'info initing home_away_sensor "Home Occupied"',
                'info initing temp_sensor (P) "Nursery"',
                'info initing temp_sensor (P) "Master Bedroom"',
                'info initing temp_sensor (P) "Living Room"'
            ]);
            if (process.env.NEST_START_TRACE) {
                fs.writeFileSync(process.env.NEST_START_TRACE, JSON.stringify({ calls: s.calls, lines: s.lines, uuids: s.api.registered.map(el => el.UUID).sort() }, null, 1));
            }
            assert.deepStrictEqual(s.api.registered.map(el => el.UUID).sort(), s.cached.map(el => el.UUID).sort(), 'the same accessories as cached');
            assert.ok(s.api.registered.every(el => !s.cached.includes(el)), 'each replaced by the one built from the list');
            // Later data updates the accessories
            const lookup = s.platform.accessoryLookup.find(el => el.deviceId == 'T1');
            assert.ok(Math.abs(lookup.device.current_temperature - 21.5) < 1e-6);
            await s.stop();
        }

        // The first stream delivers nothing and ends with a bare status 4 (deadline exceeded) ~17 s after it was opened,
        // as on 2026-10-10 18:05Z; the list comes with the next stream
        {
            const s = await start();
            (await s.nextStream()).send(statusFrame(s.conn, 4));
            const second = await s.nextStream();
            await sleep(20);
            assert.deepStrictEqual(s.calls, [], 'nothing removed, nothing built, while there is no list');
            assert.deepStrictEqual(s.names(), NAMES, 'the cached accessories are still registered');
            assert.deepStrictEqual(noticeLines(s.lines), ['info Nest ended the device stream before sending the device list (status code 4) - trying again.']);
            second.send(resourcesFrame(HOUSE));
            second.send(listFrame(s.conn, HOUSE));
            await until(() => s.calls.length == 2, 'the accessories');
            assert.deepStrictEqual(s.calls, ['unregister ' + NAMES.join(', '), 'register ' + NAMES.join(', ')]);
            assert.deepStrictEqual(s.platform.accessoryLookup.map(el => el.name).sort(), NAMES, 'all set up');
            await s.stop();
        }

        // The first message only, then the stream dies: by a status, by the session closing, by an error (twice). Then
        // the list.
        {
            const s = await start();
            let stream = await s.nextStream();
            stream.send(resourcesFrame(HOUSE));
            stream.send(statusFrame(s.conn, 4));
            stream = await s.nextStream();
            stream.send(resourcesFrame(HOUSE));
            stream.close();
            stream = await s.nextStream();
            stream.send(resourcesFrame(HOUSE));
            stream.fail(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }));
            stream = await s.nextStream();
            stream.fail(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }));
            stream = await s.nextStream();
            await sleep(20);
            assert.deepStrictEqual(s.calls, []);
            assert.deepStrictEqual(s.names(), NAMES);
            assert.deepStrictEqual(noticeLines(s.lines), [
                'info Nest ended the device stream before sending the device list (status code 4) - trying again.',
                'info Nest ended the device stream before sending the device list - trying again.'
            ]);
            assert.strictEqual(s.lines.filter(line => /^error API observe: error/.test(line)).length, 2, 'the errors are logged as before');
            stream.send(resourcesFrame(HOUSE));
            stream.send(listFrame(s.conn, HOUSE));
            await until(() => s.calls.length == 2, 'the accessories');
            assert.deepStrictEqual(s.calls, ['unregister ' + NAMES.join(', '), 'register ' + NAMES.join(', ')]);
            await s.stop();
        }

        // Nothing at first, and the list comes late: the wait is logged, and from the first notice on the cached
        // accessories report a fault instead of the values stored at the last shutdown
        {
            const s = await start();
            s.platform.deviceListWaitNoticeSeconds = 0.05;
            const cachedThermostat = s.cached[0].hapAccessory.getService(Service.Thermostat);
            const temperature = cachedThermostat.getCharacteristic(Characteristic.CurrentTemperature);
            const setpoint = cachedThermostat.getCharacteristic(Characteristic.HeatingThresholdTemperature);
            (await s.nextStream()).close();
            (await s.nextStream()).close();
            const stream = await s.nextStream();
            // Before the first notice nothing handles the cached accessories: HomeKit serves the stored value, and takes
            // a write that goes nowhere (what every start does for its first seconds)
            assert.strictEqual(await temperature.handleGetRequest(), 19.5);
            await setpoint.handleSetRequest(18.5);
            assert.strictEqual(s.conn.mergeUpdates.length, 0);
            await until(() => noticeLines(s.lines).filter(line => /Still waiting/.test(line)).length >= 2, 'two notices');
            assert.ok(/^warn Still waiting for the device list from Nest after \d+ seconds\. The 6 cached accessories are kept and report a fault until it arrives\.$/.test(
                noticeLines(s.lines).find(line => /Still waiting/.test(line))), noticeLines(s.lines).join('\n'));
            assert.strictEqual(await rejects(temperature.handleGetRequest()), FAILURE, 'reads fault while waiting');
            assert.strictEqual(await rejects(setpoint.handleSetRequest(19)), FAILURE, 'writes are refused while waiting');
            assert.deepStrictEqual(s.calls, []);
            assert.deepStrictEqual(s.names(), NAMES);
            stream.send(resourcesFrame(HOUSE));
            stream.send(listFrame(s.conn, HOUSE));
            await until(() => s.calls.length == 2, 'the accessories');
            assert.deepStrictEqual(s.calls, ['unregister ' + NAMES.join(', '), 'register ' + NAMES.join(', ')]);
            assert.ok(/^info Device list received from Nest after \d+ seconds\.$/.test(noticeLines(s.lines).pop()));
            const notices = noticeLines(s.lines).length;
            await sleep(150);
            assert.strictEqual(noticeLines(s.lines).length, notices, 'no notice once the list is there');
            // The accessories built from the list work
            const built = s.platform.accessoryLookup.find(el => el.deviceId == 'T1');
            assert.ok(Math.abs(await built.getService(Service.Thermostat).getCharacteristic(Characteristic.CurrentTemperature).handleGetRequest() - 21.5) < 1e-6);
            await s.stop();
        }

        // A cached device the list really does not have is removed, the others are kept
        {
            const s = await start();
            const stream = await s.nextStream();
            const without = HOUSE.filter(device => device.id != 'K3' && device.id != 'T2');
            stream.send(resourcesFrame(without));
            stream.send(listFrame(s.conn, without));
            await until(() => s.calls.length == 2, 'the accessories');
            const kept = NAMES.filter(name => name != 'Living Room' && name != 'Downstairs Thermostat');
            assert.deepStrictEqual(s.calls, ['unregister ' + NAMES.join(', '), 'register ' + kept.join(', ')]);
            assert.deepStrictEqual(s.names(), kept);
            await s.stop();
        }

        // observe() itself: a stream that ends without data does not resolve it, data does, and later streams' data goes
        // to the handler
        {
            const conn = new Connection({}, { info() {}, debug() {}, error() {}, warn() {} }, false, false);
            conn.connected = true;
            conn.token = 'token';
            const handled = [];
            let streams = 0, openThird;
            const third = new Promise(resolve => openThird = resolve);
            conn.updateProtobufData = async resolve => {
                streams++;
                conn.observeStreamSeq++;
                if (streams == 1) {
                    conn.lastProtobufCode = { code: 4 };
                } else if (streams == 3) {
                    await third;
                    resolve({ stream: 3 });
                } else if (streams == 4) {
                    resolve({ stream: 4 });
                } else if (streams > 4) {
                    await new Promise(() => {});
                }
            };
            let resolvedWith;
            conn.observe(data => handled.push(data)).then(data => resolvedWith = data);
            await until(() => streams == 3, 'the third stream');
            await sleep(10);
            assert.strictEqual(resolvedWith, undefined, 'still waiting after two streams without data');
            openThird();
            await until(() => streams == 5, 'the fifth stream');
            assert.deepStrictEqual(resolvedWith, { stream: 3 });
            assert.deepStrictEqual(handled, [{ stream: 4 }]);
            assert.strictEqual(conn.lastProtobufCode, null);
        }
    } finally {
        http2.connect = realConnect;
        Promise.delay = realDelay;
    }
};
