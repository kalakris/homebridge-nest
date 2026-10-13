'use strict';

// A house on both of Nest's APIs, for two-input.test.js and two-input-fuzz.js (and, from outside the repo, for another
// version of the plugin: every module of the plugin comes in through create()).
//
// Two thermostats and three temperature sensors, as the observe stream mounts them, and the sensors' objects on the
// REST API as well. Everything of the plugin is real: Connection.subscribe / dataTimerLoop / updateData /
// createNestBody for the REST API, Connection.observe / protobufDataTimerLoop / updateProtobufData and the protobuf
// decoding for the stream, the accessories, HAP-NodeJS, and for writes commitUpdate / pushUpdates. Replaced: axios'
// adapter (the REST long poll and the protobuf update request, answered here), http2.connect (each call is one observe
// stream, fed here with encoded frames), authentication, and the clock (Date.now and the global timers).
//
// The test says what Nest sends and when: house.rest() answers the long poll with changed objects, house.frame()
// sends a message on the open stream, house.snapshot() the two messages a new stream opens with.

const EventEmitter = require('events');
const fs = require('fs');
const http2 = require('http2');
const os = require('os');
const path = require('path');
const varint = require('varint');

const THERMOSTAT = 'nest.resource.NestOnyxResource', SENSOR = 'nest.resource.NestKryptoniteResource';
const THERMOSTATS = ['T1', 'T2'], SENSORS = ['K1', 'K2', 'K3'];
// Which thermostat each sensor belongs to, and the rooms
const SENSORS_OF = { T1: ['K1', 'K2'], T2: ['K3'] };
const WHERES = { T1: 'Upstairs', T2: 'Downstairs', K1: 'Nursery', K2: 'Master Bedroom', K3: 'Living Room' };
const SETPOINTS = 'nest.trait.hvac.TargetTemperatureSettingsTrait';

// ---- Fake clock (as write-fuzz.js has it)

function makeClock() {
    const real = { setTimeout: setTimeout, clearTimeout: clearTimeout, setInterval: setInterval, clearInterval: clearInterval, setImmediate: setImmediate, now: Date.now, random: Math.random };
    const clock = { now: 0, timers: new Map(), next: 1, real: real, random: real.random };
    const handle = id => ({ fakeId: id, ref() { return this; }, unref() { return this; }, hasRef() { return true; }, refresh() { return this; }, [Symbol.toPrimitive]() { return id; } });
    const isFake = h => h && typeof h == 'object' && 'fakeId' in h;
    const set = (fn, ms, args, interval) => {
        const id = clock.next++;
        clock.timers.set(id, { at: clock.now + Math.max(interval ? 1 : 0, Number(ms) || 0), fn: fn, args: args, id: id, interval: interval ? Math.max(1, Number(ms) || 1) : 0 });
        return handle(id);
    };
    const clear = which => h => {
        if (isFake(h)) {
            clock.timers.delete(h.fakeId);
        } else if (h !== null && h !== undefined) {
            which(h);
        }
    };
    clock.install = () => {
        global.setTimeout = (fn, ms, ...args) => set(fn, ms, args, false);
        global.setInterval = (fn, ms, ...args) => set(fn, ms, args, true);
        global.clearTimeout = clear(real.clearTimeout);
        global.clearInterval = clear(real.clearInterval);
        Date.now = () => clock.now;
        Math.random = () => clock.random();
    };
    clock.uninstall = () => {
        global.setTimeout = real.setTimeout;
        global.setInterval = real.setInterval;
        global.clearTimeout = real.clearTimeout;
        global.clearInterval = real.clearInterval;
        Date.now = real.now;
        Math.random = real.random;
    };
    // Lets promises and setImmediate callbacks run
    clock.settle = async () => {
        for (let i = 0; i < 6; i++) {
            await new Promise(resolve => real.setImmediate(resolve));
        }
    };
    // Runs the timers due until `target`, one at a time
    clock.runTo = async target => {
        for (;;) {
            await clock.settle();
            let next = null;
            for (const timer of clock.timers.values()) {
                if (timer.at <= target && (!next || timer.at < next.at || (timer.at == next.at && timer.id < next.id))) {
                    next = timer;
                }
            }
            if (!next) {
                break;
            }
            clock.now = Math.max(clock.now, next.at);
            if (next.interval) {
                next.at = clock.now + next.interval;
            } else {
                clock.timers.delete(next.id);
            }
            next.fn(...next.args);
        }
        clock.now = Math.max(clock.now, target);
        await clock.settle();
    };
    return clock;
}

const copy = value => JSON.parse(JSON.stringify(value));
// One length-delimited protobuf field
const field = (number, payload) => Buffer.concat([Buffer.from(varint.encode((number << 3) | 2)), Buffer.from(varint.encode(payload.length)), Buffer.from(payload)]);

// deps: { Connection, harness (test/harness.js), axios, protobuf, protoPath } of the version of the plugin to run.
// Returns { prepare(), open(options), dispose() }: prepare before the first house, dispose after the last. One house
// at a time.
function create(deps) {
    const { Connection, harness, axios, protobuf } = deps;
    const { Service, Characteristic } = harness.hap;
    const clock = makeClock();
    const env = { root: null, load: null, adapter: null, connect: null, dir: null, house: null, warn: null, rejection: null };

    async function prepare() {
        env.root = await protobuf.load(deps.protoPath);
        env.load = protobuf.load;
        // Each Connection loads the definitions (which takes real timers and a while): hand it these
        protobuf.load = () => Promise.resolve(env.root);
        env.adapter = axios.defaults.adapter;
        axios.defaults.adapter = config => env.house.request(config);
        env.connect = http2.connect;
        http2.connect = () => env.house.connect();
        env.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-two-input-'));
        env.rejection = reason => env.house && env.house.problems.push('unhandled rejection: ' + (reason && reason.stack || reason));
        process.on('unhandledRejection', env.rejection);
        // HAP-NodeJS warns on the console about values an accessory starts with
        env.warn = console.warn;
        console.warn = () => {};
        clock.install();
    }

    function dispose() {
        clock.uninstall();
        console.warn = env.warn;
        process.removeListener('unhandledRejection', env.rejection);
        protobuf.load = env.load;
        axios.defaults.adapter = env.adapter;
        http2.connect = env.connect;
        fs.rmSync(env.dir, { recursive: true, force: true });
    }

    // ---- Frames of the observe stream

    const get = (id, key, type, value) => {
        const Type = env.root.lookupType(type);
        return { object: { id: id, key: key }, data: { property: { type_url: 'type.nestlabs.com/' + type, value: Type.encode(Type.fromObject(value)).finish() } } };
    };
    const traits = {
        temperature: (id, value) => get('DEVICE_' + id, 'current_temperature', 'nest.trait.sensor.TemperatureTrait', { temperature: { value: { value: value } } }),
        backplate: (id, value) => get('DEVICE_' + id, 'backplate_temperature', 'nest.trait.sensor.TemperatureTrait', { temperature: { value: { value: value } } }),
        humidity: (id, value) => get('DEVICE_' + id, 'current_humidity', 'nest.trait.sensor.HumidityTrait', { humidity: { value: { value: value } } }),
        hvac: (id, state) => get('DEVICE_' + id, 'hvac_control', 'nest.trait.hvac.HvacControlTrait', { settings: { isCooling: state == 'cooling' ? 1 : 0, isHeating: state == 'heating' ? 1 : 0 } }),
        setpoints: (id, state) => get('DEVICE_' + id, 'target_temperature_settings', SETPOINTS,
            { settings: { hvacMode: state.mode || 'RANGE', targetTemperatureHeat: { value: state.heat }, targetTemperatureCool: { value: state.cool } }, active: { value: 1 } }),
        // A trait the plugin does nothing with (two of three messages on a real stream are such)
        unrelated: id => ({ object: { id: 'DEVICE_' + (id || 'C1'), key: 'activity' }, data: { property: { type_url: 'type.nestlabs.com/nest.trait.unknown.ActivityTrait', value: Buffer.from([8, 1]) } } })
    };
    const envStreamBody = () => env.root.lookupType('nest.rpc.StreamBody');
    const encode = gets => Buffer.from(envStreamBody().encode(envStreamBody().fromObject({ message: [{ get: gets }] })).finish());
    // A message without anything in it (Nest sends one about every 8 minutes)
    const noopFrame = () => Buffer.from(envStreamBody().encode(envStreamBody().fromObject({ noop: [Buffer.from([1])] })).finish());
    // The first message of every stream: the resources and their trait types, no values. The plugin's schema does not
    // describe it; it must do nothing.
    const resourcesFrame = () => field(1, Buffer.concat(THERMOSTATS.concat(SENSORS).map(id => field(1, Buffer.concat([
        field(1, 'DEVICE_' + id), field(2, THERMOSTATS.includes(id) ? THERMOSTAT : SENSOR),
        field(4, Buffer.concat([field(1, 'current_temperature'), field(2, 'nest.trait.sensor.TemperatureTrait')]))
    ])))));
    // The second: every trait's current value, the device list (peer_devices) among them. state: { T1: { heat, cool,
    // temperature, backplate, humidity, hvac }, ..., K1: { temperature }, ... }
    const listFrame = state => {
        const devices = THERMOSTATS.concat(SENSORS);
        const gets = [
            get('USER_U1', 'user_info', 'nest.trait.user.UserInfoTrait', { legacyId: 'user.1' }),
            get('STRUCTURE_SX', 'structure_info', 'nest.trait.structure.StructureInfoTrait', { legacyId: 'structure.S1' }),
            get('STRUCTURE_SX', 'located_annotations', 'nest.trait.located.LocatedAnnotationsTrait',
                { annotations: devices.map(id => ({ info: { id: { value: 'W' + id }, name: { value: WHERES[id] } } })) }),
            get('STRUCTURE_SX', 'peer_devices', 'weave.trait.peerdevices.PeerDevicesTrait',
                { devices: devices.map(id => ({ data: { deviceId: { value: 'DEVICE_' + id }, deviceType: { value: THERMOSTATS.includes(id) ? THERMOSTAT : SENSOR }, fwVersion: '1.0' } })) })
        ];
        devices.forEach(id => {
            const resource = 'DEVICE_' + id;
            gets.push(get(resource, 'liveness', 'weave.trait.heartbeat.LivenessTrait', { status: 'LIVENESS_DEVICE_STATUS_ONLINE' }));
            gets.push(get(resource, 'device_located_settings', 'nest.trait.located.DeviceLocatedSettingsTrait', { whereId: { value: 'W' + id } }));
            gets.push(get(resource, 'device_identity', 'weave.trait.description.DeviceIdentityTrait', { modelName: { value: 'Model' }, serialNumber: 'SERIAL' + id, fwVersion: '1.0' }));
            gets.push(traits.temperature(id, state[id].temperature));
            if (THERMOSTATS.includes(id)) {
                gets.push(get(resource, 'hvac_equipment_capabilities', 'nest.trait.hvac.HvacEquipmentCapabilitiesTrait', { canCool: 1, canHeat: 1 }));
                gets.push(traits.hvac(id, state[id].hvac));
                gets.push(traits.setpoints(id, state[id]));
                gets.push(get(resource, 'display_settings', 'nest.trait.hvac.DisplaySettingsTrait', { units: 'DEGREES_C' }));
                gets.push(get(resource, 'remote_comfort_sensing_settings', 'nest.trait.hvac.RemoteComfortSensingSettingsTrait',
                    { associatedRcsSensors: SENSORS_OF[id].map(sensor => ({ deviceId: { resourceId: 'DEVICE_' + sensor } })) }));
                gets.push(traits.backplate(id, state[id].backplate));
                gets.push(traits.humidity(id, state[id].humidity));
            }
        });
        return encode(gets);
    };

    // options.state: what both APIs start with (see listFrame; a sensor's REST reading is its temperature too).
    // options.onRequest(request): called for each change the plugin sends to Nest ({ at, set: [{ id, key, value }] });
    // returns the answer's delay in ms (default 300).
    async function open(options) {
        options = options || {};
        clock.timers.clear();
        clock.now = Date.UTC(2026, 9, 12, 17, 0, 0);
        const start = clock.now;
        clock.random = (() => {
            let a = 12345;
            return () => (a = (a * 1103515245 + 12345) % 2147483648) / 2147483648;
        })();
        const state = copy(options.state || {
            T1: { heat: 19, cool: 23, temperature: 21.3, backplate: 20.9, humidity: 50, hvac: 'off' },
            T2: { heat: 18.5, cool: 23.5, temperature: 20.4, backplate: 20.1, humidity: 52, hvac: 'off' },
            K1: { temperature: 21.3 }, K2: { temperature: 20.4 }, K3: { temperature: 19.6 }
        });

        const house = env.house = {
            clock: clock, traits: traits, state: state, problems: [], lines: [], events: [], requests: [], streams: [], stream: null, onStream: null,
            at: () => (clock.now - start) / 1000
        };
        const logger = {};
        for (const level of ['info', 'warn', 'error']) {
            logger[level] = (...args) => house.lines.push({ at: house.at(), level: level, text: args.map(el => typeof el == 'string' ? el : JSON.stringify(el)).join(' ') });
        }
        logger.debug = () => {};

        // ---- The REST API: the structure and the sensors (no thermostat is on it, as in the house this was measured in)
        let revision = 1;
        const restObject = (key, value) => ({ object_key: key, object_revision: revision++, object_timestamp: clock.now, value: value });
        const rest = house.restObjects = {
            'structure.S1': restObject('structure.S1', { name: 'Home', away: false, swarm: SENSORS.map(id => 'kryptonite.' + id), user: 'user.1' }),
            'where.S1': restObject('where.S1', { wheres: SENSORS.map(id => ({ where_id: 'W' + id, name: WHERES[id] })) })
        };
        SENSORS.forEach(id => {
            rest['kryptonite.' + id] = restObject('kryptonite.' + id, { current_temperature: state[id].temperature, battery_level: 100, where_id: 'W' + id, serial_number: 'SERIAL' + id, structure_id: 'S1', last_updated_at: Math.floor(clock.now / 1000) });
        });
        let parked = null;
        const answer = (resolve, config, objects) => resolve({ data: { objects: copy(objects) }, status: 200, statusText: 'OK', headers: {}, config: config });
        house.polls = 0;
        // Changes an object on the REST API (patch: the fields of its value that change, none for an answer that
        // carries it as it is) and answers the long poll with it. If none is open (the plugin asks again 0.1 s after
        // an answer), the next one is answered at once: this waits for it.
        house.rest = async (key, patch) => {
            Object.assign(rest[key].value, copy(patch || {}));
            rest[key].object_revision = revision++;
            rest[key].object_timestamp = clock.now;
            if (parked) {
                const { resolve, config } = parked;
                parked = null;
                answer(resolve, config, [rest[key]]);
                await clock.settle();
            } else {
                await clock.runTo(clock.now + 101);
            }
        };

        // ---- Requests
        const TraitMap = env.root.lookupType('nest.rpc.NestMessage');
        house.request = config => new Promise((resolve, reject) => {
            const url = String(config.url);
            if (/\/app_launch$/.test(url)) {
                resolve({ data: { updated_buckets: copy(Object.values(rest)) }, status: 200, statusText: 'OK', headers: {}, config: config });
            } else if (/\/v5\/subscribe$/.test(url)) {
                house.polls++;
                const known = {};
                (typeof config.data == 'string' ? JSON.parse(config.data) : config.data).objects.forEach(el => known[el.object_key] = el.object_revision);
                const newer = Object.values(rest).filter(el => el.object_revision != known[el.object_key]);
                if (newer.length) {
                    answer(resolve, config, newer);
                } else {
                    const poll = parked = { resolve: resolve, config: config };
                    setTimeout(() => {
                        if (parked === poll) {
                            parked = null;
                            reject(Object.assign(new Error('timeout'), { code: 'ECONNABORTED', config: config }));
                        }
                    }, config.timeout || 120000);
                }
            } else if (/BatchUpdateState$/.test(url)) {
                const request = { at: house.at(), set: TraitMap.decode(config.data).set.map(el => {
                    const type = env.root.lookupType(el.property.type_url.split('/')[1]);
                    return { id: el.object.id.replace('DEVICE_', ''), key: el.object.key, value: type.toObject(type.decode(el.property.value), { enums: String, defaults: true }) };
                }) };
                house.requests.push(request);
                const ms = options.onRequest ? options.onRequest(request) : 300;
                setTimeout(() => resolve({ data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config }), ms === undefined ? 300 : ms);
            } else {
                house.problems.push('an unexpected request to ' + url);
                reject(new Error('unexpected request'));
            }
        });

        // ---- The observe stream: each http2.connect is one stream
        house.connect = () => {
            const client = new EventEmitter(), request = new EventEmitter();
            const stream = { open: false, number: house.streams.length + 1 };
            const ended = () => {
                if (stream.open) {
                    stream.open = false;
                    if (house.stream === stream) {
                        house.stream = null;
                    }
                }
            };
            client.destroy = ended;
            // (An acknowledged ping: contact with Nest, and the stream's timeout starts again)
            client.ping = (payload, callback) => callback(null, 1, payload);
            client.request = () => request;
            request.write = () => {};
            request.destroy = ended;
            request.end = () => {
                stream.open = true;
                stream.send = frame => stream.open && request.emit('data', frame);
                stream.close = () => stream.open && client.emit('close');
                house.streams.push(stream);
                house.stream = stream;
                if (house.onStream) {
                    house.onStream(stream);
                }
            };
            return client;
        };
        // Sends one message with these traits on the open stream (nothing if there is none)
        house.frame = async (...gets) => {
            if (house.stream) {
                house.stream.send(encode(gets));
            }
            await clock.settle();
        };
        house.noop = async () => {
            if (house.stream) {
                house.stream.send(noopFrame());
            }
            await clock.settle();
        };
        // What a new stream opens with: Nest's full current state (house.state)
        house.snapshot = async () => {
            if (house.stream) {
                const stream = house.stream;
                stream.send(resourcesFrame());
                stream.send(listFrame(house.state));
            }
            await clock.settle();
        };
        // Ends the open stream, as Nest does about once an hour; the plugin opens the next 0.1 s later
        house.endStream = async () => {
            if (house.stream) {
                house.stream.close();
            }
            await clock.settle();
        };
        house.runTo = seconds => clock.runTo(start + Math.round(seconds * 1000));
        house.advance = ms => clock.runTo(clock.now + ms);

        // ---- The plugin, started as index.js starts it
        const conn = house.conn = new Connection({}, logger, false, false, env.dir);
        await clock.settle();
        conn.connected = true;
        conn.token = 'token';
        conn.transport_url = 'https://transport.invalid';
        conn.userid = 'user';
        conn.authAttempt = async () => true;
        const accessories = house.accessories = {};
        const handleUpdates = data => {
            for (const id in accessories) {
                const device = data.devices[accessories[id].deviceGroup][id];
                if (device) {
                    accessories[id].updateData(device, data.structures[device.structure_id]);
                }
            }
        };
        let ready = 0;
        conn.subscribe(handleUpdates).then(() => ready++);
        await clock.settle();
        conn.observe(handleUpdates).then(() => ready++);
        await clock.settle();
        await house.snapshot();
        if (ready != 2) {
            throw new Error('the house did not start');
        }
        const tree = conn.apiResponseToObjectTree(conn.currentState);
        THERMOSTATS.forEach(id => accessories[id] = harness.makeThermostat(tree.devices.thermostats[id], [], conn));
        SENSORS.forEach(id => accessories[id] = harness.makeSensor(tree.devices.temp_sensors[id], [], conn));
        conn.accessories = accessories;

        // ---- HomeKit
        const watched = { temperature: Characteristic.CurrentTemperature, low: Characteristic.HeatingThresholdTemperature, high: Characteristic.CoolingThresholdTemperature,
            mode: Characteristic.TargetHeatingCoolingState, hvac: Characteristic.CurrentHeatingCoolingState, humidity: Characteristic.CurrentRelativeHumidity };
        const characteristic = (id, name) => accessories[id].getService(THERMOSTATS.includes(id) ? Service.Thermostat : Service.TemperatureSensor).getCharacteristic(watched[name]);
        for (const id in accessories) {
            for (const name of THERMOSTATS.includes(id) ? Object.keys(watched) : ['temperature']) {
                characteristic(id, name).on('change', change => {
                    if (change.oldValue !== change.newValue) {
                        house.events.push({ at: house.at(), id: id, name: name, value: change.newValue });
                    }
                });
            }
        }
        // What a controller is shown: the value HomeKit holds (every change of it is sent as an event)
        house.shown = (id, name) => characteristic(id, name || 'temperature').value;
        // What a controller that asks reads (the get handler, with HAP's wire rounding)
        house.read = (id, name) => harness.readWire(accessories[id].getService(THERMOSTATS.includes(id) ? Service.Thermostat : Service.TemperatureSensor), watched[name || 'temperature']);
        house.write = async (id, name, value) => {
            characteristic(id, name).handleSetRequest(value, undefined).then(() => {}, error => house.problems.push('a write was refused: ' + error));
            await clock.settle();
        };
        // The events of one characteristic so far: its values in order
        house.values = (id, name) => house.events.filter(el => el.id == id && el.name == (name || 'temperature')).map(el => el.value);
        // Leaves both loops parked, with no timers running
        house.close = async () => {
            conn.ensureConnected = () => new Promise(() => {});
            await house.endStream();
            clock.timers.clear();
            env.house = { request: () => new Promise(() => {}), connect: () => new EventEmitter(), problems: [] };
        };
        return house;
    }

    return { prepare: prepare, open: open, dispose: dispose, clock: clock };
}

module.exports = { create: create, THERMOSTATS: THERMOSTATS, SENSORS: SENSORS, SENSORS_OF: SENSORS_OF };
