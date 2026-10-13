'use strict';

// Randomized test of a temperature sensor's two inputs, used by two-input-fuzz.test.js (fixed seeds) and from outside
// the repo (many seeds, other versions of the plugin: everything of the plugin comes in through create()).
//
// A scenario is a list of timed operations on the house of two-input.js, with a model of Nest behind:
//   reading    a sensor's reading changes. The observe stream delivers it after a while (usually 0.1-0.3 s, sometimes
//              seconds to minutes, sometimes never: then the next stream's first message has it), the REST API after
//              its own while (0.7 s and up)
//   other      something else in a sensor's REST object changes: the REST API answers with the whole object, with the
//              reading it has
//   structure  the REST API's structure object changes (it replaces the device list)
//   unrelated, noop, own   a stream message about something else: an unhandled trait, nothing, a thermostat's reading
//   end        Nest ends the stream; the plugin opens the next, whose first message is Nest's full current state
//   write      a controller writes a setpoint; Nest applies it and reports it on the stream after a while, or never
//              (then the plugin restarts the stream after 300 s and reads its state)
//   outside    somebody else changes a setpoint at Nest
// Each API delivers a sensor's readings in order. Profile "ordered" (as measured, 7 days): neither delivers a reading
// once the plugin has been given a newer one. Profile "racy": that happens too.
//
// What is checked, after every message and answer:
//   R1  HomeKit shows, for each sensor, the reading of the last piece of news about it: a stream message that carries
//       the sensor's reading, or a REST answer whose reading is not the REST API's last. Nothing else changes it, and
//       news is shown at once.
//   R2  (ordered) HomeKit is never shown a reading older than one it has been shown.
//   R3  a thermostat's own readings are the stream's last.
//   R4  a written setpoint is shown until Nest's next report of the thermostat's setpoints; at the end, with every
//       write answered and reported or read back, HomeKit's setpoints are Nest's.
//   R5  nothing is logged above info level, no promise is left rejected, no request goes anywhere unexpected.
// In profile "racy", R2 is not checked; how often an older reading arrived as news is counted (K1: on the stream,
// K2: with a REST answer). That was so before 4.6.10-ha.9 and is not handled.
// P1, also as before and not this test's subject: the REST API's structure object replaces the device list, here with
// one that has no thermostat (and so none of their sensors), until the stream's next message puts the stream's devices
// back. A REST reading that arrives in between is not shown until then. R1 and R2 are not checked while that lasts,
// and are checked again at that next message.
// P2, also as before: somebody else changes a setpoint, a controller writes the same thermostat before Nest's report of
// that arrives, the report arrives before the write has been sent, and Nest never reports the write. Once the write
// has been sent, the plugin takes the other value, still in the stream's data, for a change made after its own, and
// shows it until a new stream. Not counted against R4.

const twoInput = require('./two-input');

const { THERMOSTATS, SENSORS } = twoInput;

function hash(text) {
    let h = 2166136261;
    for (let i = 0; i < text.length; i++) {
        h = Math.imul(h ^ text.charCodeAt(i), 16777619);
    }
    return h >>> 0;
}

function random(seed) {
    let a = seed >>> 0;
    return () => {
        a = (a + 0x6D2B79F5) >>> 0;
        let t = a;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

const between = (r, low, high) => low + r() * (high - low);
const pick = (r, list) => list[Math.floor(r() * list.length)];

function weighted(r, table) {
    let x = r() * table.reduce((sum, el) => sum + el[1], 0);
    for (const [value, weight] of table) {
        x -= weight;
        if (x < 0) {
            return value;
        }
    }
    return table[table.length - 1][0];
}

// options: { racy, writes } force that part of the profile
function generate(seed, options) {
    options = options || {};
    const r = random(hash(seed + ':ops'));
    const flip = (name, chance) => options[name] === undefined ? r() < chance : (r(), !!options[name]);
    const profile = { racy: flip('racy', 0.3), writes: flip('writes', 0.6) };
    const ops = [];
    let at = 5000;
    const count = 15 + Math.floor(r() * 60);
    const kinds = [['reading', 40], ['unrelated', 22], ['noop', 4], ['own', 8], ['other', 8], ['structure', 2], ['end', 4]]
        .concat(profile.writes ? [['write', 9], ['outside', 3]] : []);
    for (let i = 0; i < count; i++) {
        at += Math.round(weighted(r, [[0, 6], [between(r, 1, 300), 14], [between(r, 300, 3000), 25], [between(r, 3000, 40000), 35], [between(r, 40000, 200000), 20]]));
        const op = { at: at, kind: weighted(r, kinds) };
        if (op.kind == 'reading') {
            op.id = pick(r, SENSORS);
            op.stream = weighted(r, [[Math.round(between(r, 80, 300)), 72], [Math.round(between(r, 1000, 20000)), 10], [Math.round(between(r, 20000, 600000)), 12], [null, 6]]);
            op.rest = Math.round(weighted(r, [[between(r, 700, 2500), 85], [between(r, 2500, 15000), 15]]));
        } else if (op.kind == 'other') {
            op.id = pick(r, SENSORS);
        } else if (op.kind == 'own') {
            Object.assign(op, { id: pick(r, THERMOSTATS), what: pick(r, ['temperature', 'backplate', 'humidity', 'hvac']), value: Math.round(between(r, 1700, 2400)) / 100, state: pick(r, ['off', 'heating', 'cooling']) });
        } else if (op.kind == 'end') {
            op.delay = Math.round(weighted(r, [[between(r, 300, 3000), 80], [between(r, 3000, 20000), 20]]));
        } else if (op.kind == 'write' || op.kind == 'outside') {
            const low = r() < 0.5;
            Object.assign(op, { id: pick(r, THERMOSTATS), name: low ? 'low' : 'high', value: low ? 17 + 0.5 * Math.floor(r() * 8) : 22 + 0.5 * Math.floor(r() * 8) });
            op.echo = weighted(r, [[Math.round(between(r, 300, 6000)), 70], [Math.round(between(r, 6000, 150000)), 18], [null, 12]]);
        }
        ops.push(op);
    }
    return { seed: seed, profile: profile, ops: ops };
}

const near = (a, b) => typeof a == 'number' && typeof b == 'number' && Math.abs(a - b) < 1e-6;
// A sensor's readings: each has a number and a value of its own (up to 101 of them, going up and down)
const valueOf = (id, gen) => Math.round((SENSORS.indexOf(id) + 15 + 0.03 * ((gen * 37) % 101)) * 100) / 100;
const genOf = (id, value) => (Math.round((value - SENSORS.indexOf(id) - 15) / 0.03) * 71) % 101;

// deps: as twoInput.create. Returns { prepare(), run(scenario, options), dispose() }.
function create(deps) {
    const houses = twoInput.create(deps);

    // options: { trace: true } keeps what happened, for reading
    async function run(scenario, options) {
        options = options || {};
        const racy = scenario.profile.racy;
        const violations = [], trace = [];
        const happened = { news: 0, restFirst: 0, streamFirst: 0, staleCopy: 0, restRepeat: 0, restRepeatBehind: 0, snapshots: 0, snapshotNews: 0, writes: 0, requests: 0, refreshes: 0, K1: 0, K2: 0, P1: 0, P2: 0 };
        const answerMs = 300;
        const queue = [];
        let order = 0;
        const later = (at, event) => queue.push(Object.assign(event, { at: at, order: order++ }));
        const initial = {
            T1: { heat: 19, cool: 23, temperature: 21.3, backplate: 20.9, humidity: 50, hvac: 'off' },
            T2: { heat: 18.5, cool: 23.5, temperature: 20.4, backplate: 20.1, humidity: 52, hvac: 'off' }
        };
        SENSORS.forEach(id => initial[id] = { temperature: valueOf(id, 0) });
        const house = await houses.open({ state: initial, onRequest: request => {
            happened.requests++;
            // Nest applies the change when it answers, and reports it on the stream (echo) after a while, or never
            later(house.clock.now + answerMs, { kind: 'applied', request: request });
            return answerMs;
        } });
        const clock = house.clock, t = house.traits, state = house.state;
        const violation = (rule, text) => {
            if (!violations.some(el => el.rule == rule)) {
                violations.push({ rule: rule, at: house.at(), text: text });
            }
        };
        const note = text => options.trace && trace.push(house.at().toFixed(3) + ' ' + text);

        // ---- The model
        // Per sensor: every reading has a number (gen) and a value of its own. truth: Nest's; given: the newest the
        // plugin has been handed; news: the last piece of news; restHas: what the REST API's object holds;
        // streamHas: the last the stream delivered
        const world = {};
        SENSORS.forEach(id => {
            const first = { gen: 0, value: state[id].temperature };
            world[id] = { truth: first, given: 0, news: first, restHas: first, streamHas: first, count: 0, shown: 0, lastStream: 0, lastRest: 0 };
        });
        let streamReady = true, snapshotDue = false;
        house.onStream = stream => {
            // (A stream the plugin opened: its first message comes after a while)
            streamReady = false;
            if (!snapshotDue) {
                // The plugin restarted the stream itself (a write not reported for 300 s)
                happened.refreshes++;
                later(clock.now + 1500, { kind: 'snapshot', stream: stream.number });
            } else {
                later(clock.now + snapshotDue.delay, { kind: 'snapshot', stream: stream.number });
            }
            snapshotDue = false;
        };
        const own = {}, held = {};
        THERMOSTATS.forEach(id => {
            own[id] = { temperature: state[id].temperature, backplate: state[id].backplate, humidity: state[id].humidity, hvac: state[id].hvac };
            held[id] = {};
        });
        const hvacValue = { off: 0, heating: 1, cooling: 2 };

        const newsOf = (id, reading, source) => {
            const w = world[id];
            happened.news++;
            if (reading.gen < w.news.gen) {
                happened[source == 'stream' ? 'K1' : 'K2']++;
                if (!racy) {
                    violation('model', 'the model delivered an older reading as news in profile ordered');
                }
            }
            w.news = reading;
            w.given = Math.max(w.given, reading.gen);
        };

        // ---- What must hold after every message and answer
        // (P1: whether the device list is the REST API's)
        let listReplaced = false;
        const check = what => {
            for (const id of SENSORS) {
                const w = world[id], shown = house.shown(id);
                if (listReplaced) {
                    if (!near(shown, w.news.value)) {
                        happened.P1++;
                    }
                    continue;
                }
                if (!near(shown, w.news.value)) {
                    violation('R1', 'after ' + what + ': ' + id + ' is shown ' + shown + ' (reading ' + genOf(id, shown) + '), the last news about it is ' + w.news.value + ' (reading ' + w.news.gen + ')');
                }
                const gen = genOf(id, shown);
                if (!racy && gen < w.shown) {
                    violation('R2', 'after ' + what + ': ' + id + ' is shown reading ' + gen + ' (' + shown + ') after reading ' + w.shown);
                }
                w.shown = racy ? gen : Math.max(w.shown, gen);
            }
            for (const id of THERMOSTATS) {
                for (const name of ['temperature', 'humidity']) {
                    if (!near(house.shown(id, name), own[id][name])) {
                        violation('R3', 'after ' + what + ': ' + id + ' ' + name + ' is shown ' + house.shown(id, name) + ', the stream\'s last is ' + own[id][name]);
                    }
                }
                if (house.shown(id, 'hvac') !== hvacValue[own[id].hvac]) {
                    violation('R3', 'after ' + what + ': ' + id + ' is shown hvac state ' + house.shown(id, 'hvac') + ', the stream\'s last is ' + own[id].hvac);
                }
                for (const name in held[id]) {
                    // (For 300 s from the push, 2 s or more after the write: then the plugin restarts the stream to
                    // read Nest's state. Not checked from shortly before that: a message that arrives in the 10 ms
                    // between that limit and the timer for it is shown without the written value, as before, until
                    // the new stream's first message)
                    if (clock.now < held[id][name].at + 295000 && !near(house.shown(id, name), held[id][name].value)) {
                        violation('R4', 'after ' + what + ': ' + id + ' ' + name + ' is shown ' + house.shown(id, name) + ' while the written ' + held[id][name].value + ' has not been reported');
                    }
                }
            }
            if (house.problems.length) {
                violation('R5', house.problems[0]);
            }
            const line = house.lines.find(el => el.level != 'info');
            if (line) {
                violation('R5', 'logged at ' + line.level + ' level: ' + line.text);
            }
        };

        // ---- The operations and what follows from them
        // (The writes Nest has not been sent yet: when it reports them, once applied)
        const pendingEcho = [];
        // (P2: the thermostats whose setpoints Nest reported while a write to them had not been sent yet)
        const raced = {};
        const usable = number => streamReady && house.stream && house.stream.number == number;
        const apply = async event => {
            const kind = event.kind;
            if (kind == 'reading') {
                const w = world[event.id];
                const reading = w.truth = { gen: ++w.count, value: valueOf(event.id, w.count) };
                state[event.id].temperature = reading.value;
                note('reading ' + event.id + ' #' + reading.gen + ' = ' + reading.value + ' (stream after ' + event.stream + ' ms, REST after ' + event.rest + ' ms)');
                if (event.stream !== null && house.stream) {
                    w.lastStream = Math.max(clock.now + event.stream, w.lastStream + 1);
                    later(w.lastStream, { kind: 'streamReading', id: event.id, reading: reading, stream: house.stream.number });
                }
                w.lastRest = Math.max(clock.now + event.rest, w.lastRest + 1);
                later(w.lastRest, { kind: 'restReading', id: event.id, reading: reading });
            } else if (kind == 'streamReading') {
                const w = world[event.id];
                if (!usable(event.stream) || (!racy && event.reading.gen < w.given)) {
                    return;
                }
                if (event.reading.gen > w.given) {
                    happened.streamFirst++;
                }
                note('the stream delivers ' + event.id + ' #' + event.reading.gen);
                newsOf(event.id, event.reading, 'stream');
                w.streamHas = event.reading;
                listReplaced = false;
                await house.frame(t.temperature(event.id, event.reading.value));
                check('the stream\'s reading of ' + event.id);
            } else if (kind == 'restReading') {
                const w = world[event.id];
                if (event.reading.gen <= w.restHas.gen || (!racy && event.reading.gen < w.given)) {
                    return;
                }
                if (event.reading.gen > w.given) {
                    happened.restFirst++;
                }
                note('the REST API delivers ' + event.id + ' #' + event.reading.gen);
                w.restHas = event.reading;
                newsOf(event.id, event.reading, 'rest');
                await house.rest('kryptonite.' + event.id, { current_temperature: event.reading.value });
                check('the REST API\'s reading of ' + event.id);
            } else if (kind == 'other') {
                const w = world[event.id];
                happened.restRepeat++;
                if (w.restHas.gen < w.news.gen) {
                    happened.restRepeatBehind++;
                }
                note('the REST API sends ' + event.id + '\'s object again, with #' + w.restHas.gen);
                await house.rest('kryptonite.' + event.id, { battery_level: 50 + (event.at % 50) });
                check('the REST API\'s object of ' + event.id + ' with nothing new');
            } else if (kind == 'structure') {
                note('the REST API sends the structure');
                await house.rest('structure.S1', { away: event.at % 2 == 0 });
                listReplaced = true;
                check('the REST API\'s structure');
            } else if (kind == 'unrelated' || kind == 'noop' || kind == 'own') {
                if (!usable(house.stream && house.stream.number)) {
                    return;
                }
                if (SENSORS.some(id => world[id].streamHas.gen < world[id].news.gen)) {
                    happened.staleCopy++;
                }
                note('a stream message: ' + kind);
                listReplaced = false;
                if (kind == 'own') {
                    own[event.id][event.what] = event.what == 'hvac' ? event.state : event.value;
                    state[event.id][event.what] = own[event.id][event.what];
                    await house.frame(event.what == 'hvac' ? t.hvac(event.id, event.state) : t[event.what](event.id, event.value));
                } else if (kind == 'noop') {
                    await house.noop();
                } else {
                    await house.frame(t.unrelated());
                }
                check('a stream message about something else (' + kind + ')');
            } else if (kind == 'end') {
                if (!house.stream) {
                    return;
                }
                note('Nest ends the stream');
                snapshotDue = { delay: event.delay };
                streamReady = false;
                await house.endStream();
                check('the end of a stream');
            } else if (kind == 'snapshot') {
                if (!house.stream || house.stream.number != event.stream) {
                    return;
                }
                happened.snapshots++;
                note('the new stream\'s first message');
                for (const id of SENSORS) {
                    const w = world[id];
                    if (w.truth.gen > w.given) {
                        happened.snapshotNews++;
                    }
                    newsOf(id, w.truth, 'stream');
                    w.streamHas = w.truth;
                }
                THERMOSTATS.forEach(id => held[id] = {});
                streamReady = true;
                listReplaced = false;
                await house.snapshot();
                check('a new stream\'s first message');
            } else if (kind == 'write') {
                happened.writes++;
                note('write ' + event.id + ' ' + event.name + ' = ' + event.value);
                held[event.id][event.name] = { value: event.value, at: clock.now };
                await house.write(event.id, event.name, event.value);
                pendingEcho.push({ id: event.id, name: event.name, value: event.value, echo: event.echo });
                check('a write');
            } else if (kind == 'applied') {
                // Nest has applied a request: its state, and the report of it
                for (const el of event.request.set.filter(el => el.key == 'target_temperature_settings')) {
                    state[el.id].heat = el.value.settings.targetTemperatureHeat.value;
                    state[el.id].cool = el.value.settings.targetTemperatureCool.value;
                    const echoes = pendingEcho.filter(echo => echo.id == el.id);
                    echoes.forEach(echo => pendingEcho.splice(pendingEcho.indexOf(echo), 1));
                    const delays = echoes.map(echo => echo.echo);
                    if (house.stream && delays.length && delays.every(delay => delay !== null)) {
                        later(clock.now + Math.max(...delays), { kind: 'setpoints', id: el.id, stream: house.stream.number });
                    }
                }
            } else if (kind == 'outside') {
                note('somebody sets ' + event.id + ' ' + event.name + ' = ' + event.value);
                state[event.id][event.name == 'low' ? 'heat' : 'cool'] = event.value;
                if (house.stream) {
                    later(clock.now + Math.min(event.echo === null ? 2000 : event.echo, 5000), { kind: 'setpoints', id: event.id, stream: house.stream.number });
                }
            } else if (kind == 'setpoints') {
                if (!usable(event.stream)) {
                    return;
                }
                note('Nest reports ' + event.id + '\'s setpoints ' + state[event.id].heat + ' / ' + state[event.id].cool);
                if (pendingEcho.some(echo => echo.id == event.id)) {
                    raced[event.id] = true;
                }
                held[event.id] = {};
                listReplaced = false;
                await house.frame(t.setpoints(event.id, state[event.id]));
                check('Nest\'s report of setpoints');
            }
        };

        scenario.ops.forEach(op => later(clock.now + op.at, Object.assign({}, op)));
        const next = () => {
            let best = null;
            for (const event of queue) {
                if (!best || event.at < best.at || (event.at == best.at && event.order < best.order)) {
                    best = event;
                }
            }
            return best;
        };
        const drain = async until => {
            for (;;) {
                const event = next();
                const target = Math.min(event ? event.at : until, until, clock.now + 1000);
                if (target > clock.now) {
                    await clock.runTo(target);
                    continue;
                }
                if (!event || event.at > clock.now) {
                    if (clock.now >= until) {
                        break;
                    }
                    continue;
                }
                queue.splice(queue.indexOf(event), 1);
                await apply(event);
            }
        };
        const last = clock.now + scenario.ops[scenario.ops.length - 1].at;
        // Until every reading has arrived and every write has been reported or read back (300 s, a new stream)
        await drain(last + 700000);
        check('the end');
        for (const id of THERMOSTATS) {
            if ((!near(house.shown(id, 'low'), state[id].heat) || !near(house.shown(id, 'high'), state[id].cool)) && raced[id]) {
                happened.P2++;
            } else if (!near(house.shown(id, 'low'), state[id].heat) || !near(house.shown(id, 'high'), state[id].cool)) {
                violation('R4', 'at the end ' + id + ' is shown ' + house.shown(id, 'low') + ' / ' + house.shown(id, 'high') + ', Nest has ' + state[id].heat + ' / ' + state[id].cool);
            }
            if (house.conn.mergeUpdates.length || house.conn.pendingUpdates.length) {
                violation('R4', 'at the end a change is still held or queued');
            }
        }
        for (const id of SENSORS) {
            if (world[id].news.gen != world[id].truth.gen && !racy) {
                violation('model', id + ': the last news is not Nest\'s reading at the end');
            }
        }
        // For comparing two versions of the plugin: everything about the thermostats and the log, and separately
        // what HomeKit was sent for the sensors
        const result = {
            violations: violations, happened: happened, trace: trace,
            thermostats: JSON.stringify([house.events.filter(el => THERMOSTATS.includes(el.id)), house.requests, house.lines]),
            sensors: JSON.stringify(house.events.filter(el => SENSORS.includes(el.id))),
            sensorEvents: house.events.filter(el => SENSORS.includes(el.id)).length
        };
        await house.close();
        return result;
    }

    return { prepare: houses.prepare, run: run, dispose: houses.dispose };
}

module.exports = { create: create, generate: generate };
