'use strict';

// Randomized test of the write path, used by write-fuzz.test.js (fixed seeds) and from outside the repo (many seeds,
// other versions of the plugin: every module of the plugin comes in through create()).
//
// A scenario is a list of timed operations and the random draws that answer what the plugin does. It drives two real
// thermostat accessories through HAP writes -> accessory -> conn.update -> commitUpdate -> debounce -> pushUpdates ->
// axios (answered here), with a model of Nest behind: it applies a request or not, depending on the answer drawn for
// it, and reports its state on the observe stream (messages fed in as the stream's data handler does). Time is a
// fake clock (Date.now and the global timers).
//
// Operations: both setpoints or one, a mode change, eco on or off, the target temperature (single modes), a change
// made at Nest by someone else, an unrelated message, a new observe stream, nest-no-resend.on created or removed.
// Gaps from none to minutes, so that writes fall inside the debounce, into an unanswered push and into a resend.
// Answers: normal (fast, or slow up to the timeout), nothing but a status 4 or a status 7 (with or without a text),
// other statuses, garbage, HTTP 401 / 403 / 400 / 500, a timeout, connection errors. A reauthentication takes a
// while, and can fail. Messages from Nest: in order, or (profile "lossy") late, never, or out of
// order; a restarted stream opens with Nest's full state.
//
// What is checked is in check() and settle() below, numbered I1 to I15.

const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');

const SETPOINTS = 'type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait';
const ECO_STATE = 'type.nestlabs.com/nest.trait.hvac.EcoModeStateTrait';
const UP = 'AAAA000000000001', DN = 'BBBB000000000002';
const IDS = [UP, DN];
const LOW = 'target_temperature_low', HIGH = 'target_temperature_high', TEMP = 'target_temperature', TYPE = 'target_temperature_type';
const NOT_APPLIED = Buffer.from('12020804', 'hex');
const FLAG_FILE = 'nest-no-resend.on';
// The plugin's API_PUSH_RESEND_MAX, API_MERGE_UNPUSHED_MAX_SECONDS and API_MERGE_ECHO_MAX_SECONDS
const RESEND_MAX = 1, UNPUSHED_MAX_MS = 120000, ECHO_MAX_MS = 300000;
// The kinds of answer (DRAWS.answer) that are an HTTP 200, and those of them that say Nest did not apply the request
const ANSWERED = ['ok', 'status4', 'status7', 'status', 'garbage'], LOST = ['status4', 'status7'];
const RETRYABLE_CODES = ['ECONNREFUSED', 'ESOCKETTIMEDOUT', 'ECONNABORTED', 'ENETUNREACH', 'ETIMEDOUT', 'ECONNRESET', 'EAI_AGAIN'];

// ---- Random numbers: every draw is a function of the seed and of what it is for, so that a scenario can be cut
// down and run again, and two versions of the plugin get the same answers as long as they send the same requests

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

// ---- Scenario

// options: { status4: false } never draws a bare status 4 or 7; { flag: 'on' } has nest-no-resend.on there throughout,
// 'never' never, otherwise it comes and goes; { errors, lossy, modes } force that part of the profile
function generate(seed, options) {
    options = options || {};
    const r = random(hash(seed + ':ops'));
    const flip = (name, chance) => options[name] === undefined ? r() < chance : (r(), !!options[name]);
    const profile = {
        errors: flip('errors', 0.5),      // requests fail (HTTP errors, timeouts, connection errors)
        lossy: flip('lossy', 0.5),        // messages from Nest come late, never, or out of order
        modes: flip('modes', 0.6),        // mode and eco changes, the target temperature
        status4: options.status4 !== false,
        flag: options.flag || 'toggle'
    };
    const ops = [];
    let at = 0;
    const count = 3 + Math.floor(r() * 24);
    const kinds = [['pair', 30], ['one', 20], ['outside', 8], ['message', 6], ['stream', 3]]
        .concat(profile.modes ? [['mode', 9], ['eco', 5], ['temperature', 5]] : [])
        .concat(profile.flag == 'toggle' ? [['flag', 6]] : []);
    const low = () => 17 + 0.5 * Math.floor(r() * 8), high = () => 22 + 0.5 * Math.floor(r() * 8);
    for (let i = 0; i < count; i++) {
        at += Math.round(weighted(r, [[0, 15], [between(r, 1, 50), 15], [between(r, 100, 2000), 20], [between(r, 2000, 20000), 25], [between(r, 20000, 90000), 15], [between(r, 120000, 420000), 10]]));
        const op = { at: at, kind: weighted(r, kinds), id: pick(r, IDS) };
        if (op.kind == 'pair') {
            Object.assign(op, { low: low(), high: high(), gap: Math.round(weighted(r, [[0, 60], [between(r, 1, 20), 15], [between(r, 200, 1900), 15], [between(r, 2500, 9000), 10]])) });
        } else if (op.kind == 'one') {
            Object.assign(op, r() < 0.5 ? { low: low() } : { high: high() });
        } else if (op.kind == 'mode') {
            op.mode = pick(r, ['heat', 'cool', 'range']);
        } else if (op.kind == 'eco') {
            op.on = r() < 0.5;
        } else if (op.kind == 'temperature') {
            op.value = 18 + 0.5 * Math.floor(r() * 12);
        } else if (op.kind == 'outside') {
            Object.assign(op, weighted(r, [[{ low: low() }, 3], [{ high: high() }, 3], [{ low: low(), high: high() }, 2], [{ mode: pick(r, ['heat', 'cool', 'range']) }, profile.modes ? 2 : 0]]));
        }
        ops.push(op);
    }
    return { seed: seed, profile: profile, ops: ops, draws: {} };
}

// The draw number `index` of a kind, kept in the scenario once made
function draw(scenario, kind, index) {
    const made = scenario.draws[kind] = scenario.draws[kind] || {};
    if (made[index] === undefined) {
        made[index] = DRAWS[kind](random(hash(scenario.seed + ':' + kind + ':' + index)), scenario.profile);
    }
    return made[index];
}

const DRAWS = {
    // How a request is answered, after how long, and whether Nest applies it (and when, as a share of that time).
    // One answered with nothing but a status 4 or 7 is never applied; what Nest does with a request it answers
    // oddly, or whose answer is lost, is not known, so both happen.
    answer(r, profile) {
        const kind = weighted(r, [['ok', 50], ['status4', profile.status4 ? 14 : 0], ['status7', profile.status4 ? 9 : 0], ['status', 6], ['garbage', 4]]
            .concat(profile.errors ? [['401', 5], ['403', 1], ['400', 1], ['500', 3], ['timeout', 3], ['ECONNRESET', 3], ['ENETUNREACH', 2]] : []));
        const seconds = {
            ok: () => r() < 0.7 ? between(r, 0.3, 5) : between(r, 5, 39),
            status4: () => r() < 0.8 ? between(r, 15, 33) : between(r, 0.2, 5),
            status7: () => r() < 0.8 ? between(r, 15, 33) : between(r, 0.2, 5),
            status: () => between(r, 0.3, 30),
            garbage: () => between(r, 0.3, 30),
            timeout: () => 40
        };
        const applied = { ok: 1, status4: 0, status7: 0, status: 0.5, garbage: 0.5, timeout: 0.3, ECONNRESET: 0.15 };
        const text = (code, words) => Buffer.concat([Buffer.from([0x12, 4 + words.length, 0x08, code, 0x12, words.length]), Buffer.from(words)]).toString('hex');
        const body = {
            // (as they came: the 4 bare, the 7 with its text; and each the other way, the text in another case)
            status4: () => weighted(r, [['12020804', 8], [text(4, 'deadline exceeded'), 2]]),
            status7: () => weighted(r, [[text(7, 'authentication failed'), 7], ['12020807', 2], [text(7, 'Authentication Failed'), 1]]),
            status: () => pick(r, ['12020805', '1202080d', text(14, 'unavailable'), '1200', '1203088400', text(7, 'authentication failed') + '00']),
            garbage: () => pick(r, ['ffffffff', '3c68746d6c3e', '0a7f01', '120208'])
        };
        return {
            kind: kind,
            ms: Math.round(1000 * (seconds[kind] || (() => between(r, 0.1, 6)))()),
            applied: r() < (applied[kind] || 0),
            share: between(r, 0.2, 1),
            body: body[kind] ? body[kind]() : ''
        };
    },
    // When Nest's message about a change arrives (ms after the change), or never
    echo(r, profile) {
        if (!profile.lossy) {
            return Math.round(between(r, 300, 6000));
        }
        return weighted(r, [[Math.round(between(r, 300, 6000)), 60], [Math.round(between(r, 6000, 120000)), 20], [Math.round(between(r, 120000, 400000)), 8], [null, 12]]);
    },
    // When a new stream delivers Nest's state (ms after the old one ended)
    stream(r) {
        return Math.round(r() < 0.8 ? between(r, 500, 25000) : between(r, 31000, 120000));
    },
    // Whether a reauthentication succeeds
    auth(r) {
        return r() < 0.75;
    },
    // ... and how long it takes (ms)
    authTime(r) {
        return Math.round(weighted(r, [[between(r, 200, 3000), 70], [between(r, 3000, 20000), 20], [between(r, 45000, 90000), 10]]));
    }
};

// ---- Fake clock

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
    // Runs the timers due until `target`, one at a time, calling each() after each has run and settled
    clock.runTo = async (target, each) => {
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
            await clock.settle();
            if (each) {
                each();
            }
        }
        clock.now = Math.max(clock.now, target);
        await clock.settle();
    };
    return clock;
}

// ---- The run

const f32 = Math.fround;
// The same value of a setting, as the plugin compares (Nest holds float32), and the very same number
const same = (a, b) => typeof a == 'number' && typeof b == 'number' ? Math.abs(a - b) < 0.01 : JSON.stringify(a) === JSON.stringify(b);
const exact = (a, b) => typeof a == 'number' && typeof b == 'number' ? Math.abs(a - b) < 1e-9 : JSON.stringify(a) === JSON.stringify(b);
const copy = value => JSON.parse(JSON.stringify(value));

// What Nest's state looks like in the plugin's data (protobufToNestLegacy)
function reported(state) {
    const type = state.active ? state.mode.toLowerCase() : 'off';
    return {
        [TYPE]: type, [LOW]: state.heat, [HIGH]: state.cool,
        [TEMP]: type == 'heat' ? state.heat : type == 'cool' ? state.cool : 0.5 * (state.cool + state.heat)
    };
}

// deps: { Connection, harness (test/harness.js), axios, protobuf, protoPath } of the version of the plugin to run.
// Returns { prepare(), run(scenario, options), dispose() }: prepare before the first run, dispose after the last.
function create(deps) {
    const { Connection, harness, axios, protobuf } = deps;
    const hap = harness.hap;
    const clock = makeClock();
    const env = { root: null, load: null, adapter: null, dir: null, world: null, rejection: null, warn: null };

    async function prepare() {
        env.root = await protobuf.load(deps.protoPath);
        env.load = protobuf.load;
        // Each Connection loads the definitions (which takes real timers and a while): hand it these
        protobuf.load = () => Promise.resolve(env.root);
        env.adapter = axios.defaults.adapter;
        axios.defaults.adapter = config => env.world.request(config);
        env.dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-write-fuzz-'));
        env.rejection = reason => {
            if (env.world) {
                env.world.violation('I7', 'unhandled rejection: ' + (reason && reason.stack || reason));
            }
        };
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
        fs.rmSync(env.dir, { recursive: true, force: true });
    }

    // options: { status4Lost: false } for a version of the plugin that takes a bare status 4 or 7 for success (then
    // such a change does not count as lost); { trace: true } keeps what happened, for reading
    async function run(scenario, options) {
        options = options || {};
        const profile = scenario.profile;
        const flagPath = path.join(env.dir, FLAG_FILE);
        const setFlag = on => on ? fs.writeFileSync(flagPath, '') : fs.rmSync(flagPath, { force: true });
        setFlag(profile.flag == 'on');
        clock.timers.clear();
        clock.now = Date.UTC(2026, 9, 11, 1, 0, 0);
        const start = clock.now;
        clock.random = random(hash(scenario.seed + ':uuid'));
        const at = () => (clock.now - start) / 1000;

        const world = env.world = {
            violations: [], lines: [], kept: [], requests: [], commits: [], steps: [], events: [],
            open: 0, model: 0, restarts: 0, auths: 0, reauthenticated: 0, echoes: 0, rejected: 0, latest: {}, sent: {}, groups: {}, reported: {}, dropped: [], taken: [], settles: 0
        };
        world.violation = (invariant, text) => {
            if (!world.violations.some(el => el.invariant == invariant)) {
                world.violations.push({ invariant: invariant, at: at(), text: text });
            }
        };
        const note = text => options.trace && world.events.push(at().toFixed(3) + ' ' + text);

        const logger = {};
        for (const level of ['info', 'warn', 'error']) {
            logger[level] = (...args) => {
                const text = args.map(el => typeof el == 'string' ? el : JSON.stringify(el)).join(' ');
                world.lines.push({ at: clock.now, level: level, text: text });
                world.kept.push(at().toFixed(3) + ' ' + level + ' ' + text);
                note(level + ' ' + text);
                if (level == 'error') {
                    lost(text);
                } else if (level == 'info') {
                    resent(text);
                }
            };
        }
        logger.debug = () => {};

        // ---- The plugin: one connection, two thermostats in range mode
        const conn = new Connection({}, logger, false, false, env.dir);
        await clock.settle();
        conn.connected = true;
        conn.token = 'token';
        conn.transport_url = 'https://transport.invalid';
        conn.userid = 'user';
        // A reauthentication, as Connection.auth: one attempt at a time, shared. It takes a while. Unless it is one
        // that keeps the session (preemptive), the plugin has none meanwhile, and pushes wait for it. It fails by
        // resolving false if it kept the session, by throwing otherwise (the plugin's retry of a failed push then
        // waits and pushes all the same).
        conn.auth = preemptive => {
            if (!conn.authPromise) {
                const index = world.auths++;
                const ok = draw(scenario, 'auth', index), ms = draw(scenario, 'authTime', index);
                note('reauthentication' + (preemptive ? ', keeping the session' : '') + ': ' + (ok ? 'succeeds' : 'fails') + ' after ' + ms / 1000 + ' s');
                if (!preemptive) {
                    conn.connected = false;
                    conn.token = null;
                }
                conn.authPromise = new Promise((resolve, reject) => later(ms, () => {
                    conn.connected = true;
                    conn.token = 'token';
                    conn.authPromise = null;
                    world.reauthenticated += ok ? 1 : 0;
                    if (ok || preemptive) {
                        resolve(ok);
                    } else {
                        reject(new Error('reauthentication failed'));
                    }
                }));
                conn.authPromise.catch(() => {});
            }
            return conn.authPromise;
        };
        const nest = {};
        const device = id => ({
            target_temperature_type: 'range', can_heat: true, can_cool: true, fan_timer_timeout: 0, eco: { mode: 'schedule' }, using_protobuf: true,
            temperature_scale: 'C', serial_number: id, hvac_heater_state: false, hvac_ac_state: false, backplate_temperature: 21.5, current_humidity: 50,
            protobuf_device_type: 'nest.resource.NestOnyxResource', where_id: 'w' + id
        });
        conn.currentState = { structure: { S1: { structure_id: 'S1', swarm: IDS.map(id => 'device.' + id), name: 'Home', away: false } }, where: { S1: { wheres: IDS.map(id => ({ where_id: 'w' + id, name: id })) } }, track: {}, device: {}, shared: {} };
        for (const id of IDS) {
            nest[id] = { mode: 'RANGE', active: 1, heat: f32(19), cool: f32(23), eco: false };
            conn.currentState.track[id] = { online: true };
            conn.currentState.device[id] = device(id);
            conn.currentState.shared[id] = Object.assign({ current_temperature: 21.3, can_heat: true, can_cool: true }, reported(nest[id]));
            conn.legacyDeviceMap[id] = 'device';
        }
        conn.legacyStructureMap.S1 = 'S1';
        conn.noteContact();
        const accessories = {};
        const tree = conn.apiResponseToObjectTree(conn.currentState);
        for (const id of IDS) {
            accessories[id] = harness.makeThermostat(tree.devices.thermostats[id], [], conn);
        }
        conn.accessories = accessories;
        conn.updateHomeKit = data => {
            for (const id of IDS) {
                accessories[id].updateData(data.devices.thermostats[id], data.structures[data.devices.thermostats[id].structure_id]);
            }
        };
        conn.observeStreamSeq = 1;
        // (An acknowledged ping on the open stream, every 60 s)
        setInterval(() => conn.noteContact(), 60000);

        const settleMergeUpdates = conn.settleMergeUpdates.bind(conn);
        conn.settleMergeUpdates = (...args) => {
            world.settles++;
            return settleMergeUpdates(...args);
        };

        // ---- What the plugin is asked to do, and what it sends
        const settingKey = commit => commit.object + '/' + commit.key;
        const commitUpdate = conn.commitUpdate.bind(conn);
        conn.commitUpdate = (nodeId, body, hvacMode, usingProtobuf) => {
            body = Object.assign({}, body);
            for (const key in body) {
                if (key != 'target_change_pending') {
                    const seq = world.commits.length;
                    if (typeof body[key] == 'number') {
                        // Every temperature written is a number of its own (by less than Nest's float32 can tell),
                        // so that a value in a request says which change it is
                        body[key] += (seq + 1) * 1e-7;
                    }
                    const commit = { seq: seq, at: clock.now, object: nodeId, key: key, value: copy(body[key]), accepted: false, dropped: false, reported: false, resent: false, flying: 0, last: null };
                    // What I1 counts: per change, or (modes and eco: a value comes back) per value of the setting
                    const name = typeof body[key] == 'number' ? '#' + seq : nodeId + '/' + key + '=' + JSON.stringify(body[key]);
                    commit.group = world.groups[name] = world.groups[name] || { changes: 0, sends: 0, errors: 0, lost: 0 };
                    commit.group.changes++;
                    world.commits.push(commit);
                    world.latest[settingKey(commit)] = commit;
                    world.reported[settingKey(commit)] = [];
                    note('commit #' + commit.seq + ' ' + nodeId + ' ' + key + '=' + JSON.stringify(body[key]));
                }
            }
            return commitUpdate(nodeId, body, hvacMode, usingProtobuf);
        };
        // The changes a push is given (as they were made: in a single mode pushUpdates sends a threshold as the target
        // temperature, and of two thresholds only one), by the bytes of the request it builds from them
        const built = new WeakMap();
        let pushing = null;
        const pushUpdates = conn.pushUpdates.bind(conn);
        conn.pushUpdates = (...args) => {
            // (Where two queued changes write the same setting, a failed push queued again in front of a newer one,
            // the request carries the first one's value: the plugin builds a request from the queue's end backwards,
            // each change writing over the ones after it. I13 checks this against the bytes.)
            const entries = {};
            copy(args[0] || conn.pendingUpdates).forEach((el, index) => {
                for (const key in el.object.value) {
                    const first = entries[el.object.object_key + '/' + key];
                    if (key != 'target_change_pending' && !first) {
                        entries[el.object.object_key + '/' + key] = { object: el.object.object_key, key: key, value: el.object.value[key], mode: el.hvac_mode };
                    } else if (first && same(first.value, el.object.value[key])) {
                        // (The same value, to what Nest can tell, asked for again: this change goes out as well)
                        entries[el.object.object_key + '/' + key + '/' + index] = { object: el.object.object_key, key: key, value: el.object.value[key], mode: el.hvac_mode };
                    } else if (first) {
                        // (For P3: a queued change whose value does not go out, written over by one queued before it)
                        const commit = commitOf({ object: el.object.object_key, key: key, value: el.object.value[key] }, world.commits.length);
                        if (commit) {
                            commit.overridden = true;
                        }
                    }
                }
            });
            pushing = Object.values(entries);
            try {
                return pushUpdates(...args);
            } finally {
                pushing = null;
            }
        };
        const legacyToProtobufUpdate = conn.legacyToProtobufUpdate.bind(conn);
        conn.legacyToProtobufUpdate = body => {
            const bytes = legacyToProtobufUpdate(body);
            if (bytes) {
                built.set(bytes, { entries: pushing || [], commits: world.commits.length });
            }
            return bytes;
        };
        // The change a value in a push is: the last one made with it before the request was built. (A failed push is
        // queued again as it was sent: a threshold as the target temperature.)
        const commitOf = (entry, before) => {
            for (let i = before - 1; i >= 0; i--) {
                const commit = world.commits[i];
                if (commit.object == entry.object && exact(commit.value, entry.value) && (commit.key == entry.key || (entry.key == TEMP && (commit.key == LOW || commit.key == HIGH)))) {
                    return commit;
                }
            }
            return null;
        };
        // The changes a piece of a log line names (as the plugin's describeUpdates writes them): [{ commit, key }]
        const named = text => {
            const found = [];
            const pattern = /(shared|device)\.(\w+) \{/g;
            for (let match = pattern.exec(text); match; match = pattern.exec(text)) {
                let depth = 0, end = match.index + match[0].length - 1;
                for (let i = end; i < text.length; i++) {
                    depth += text[i] == '{' ? 1 : text[i] == '}' ? -1 : 0;
                    if (!depth) {
                        end = i;
                        break;
                    }
                }
                let value;
                try {
                    value = JSON.parse(text.slice(match.index + match[0].length - 1, end + 1));
                } catch (error) {
                    continue;
                }
                for (const key in value) {
                    const commit = commitOf({ object: match[1] + '.' + match[2], key: key, value: value[key] }, world.commits.length);
                    if (commit) {
                        found.push({ commit: commit, key: key });
                    }
                }
            }
            return found;
        };
        // An error line that says changes were NOT applied: which ones
        const lost = text => {
            const from = text.indexOf('NOT applied');
            const givenUp = /when it was sent again/.test(text);
            named(from < 0 ? '' : text.slice(from)).forEach(({ commit, key }) => {
                commit.dropped = true;
                // I12. A change given up because Nest went on answering that it had not applied it has been sent
                // as often as a change may be: once, and RESEND_MAX times again. (Per change, so for temperatures
                // only: a mode or an eco setting comes back, and its sends are counted per value. Not for a change
                // that is lost because it was no longer held, which is not sent again at all.)
                if (givenUp && typeof commit.value == 'number' && commit.group.sends < 1 + RESEND_MAX) {
                    world.violation('I12', 'change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + ') is given up after ' + commit.group.sends + ' send(s)');
                }
                // (For I10. named: whether the line names the change by the key it was made with. In a single mode
                // a threshold is sent, and named, as the target temperature)
                world.dropped.push({ commit: commit, resend: /^Nest did not apply the change/.test(text), named: commit.key == key });
            });
        };
        // A line that says changes are sent again after a status 4: which ones (for I2: the next request carrying
        // one of them is that resend, not the plugin's retry of a failed request)
        const resent = text => {
            const from = text.indexOf(' - sending ');
            if (from >= 0 && / again/.test(text.slice(from))) {
                const whole = text.slice(from).startsWith(' - sending it again');
                named(whole ? text.slice(0, text.search(/ \(status \d after /)) : text.slice(from, text.indexOf(' again', from))).forEach(({ commit }) => {
                    commit.resent = true;
                    commit.resentAt = world.settles;
                    // I14. After a status 7 a change is sent again only once a reauthentication has succeeded since.
                    if (/ \(status 7 after /.test(text) && commit.before7 !== undefined && world.reauthenticated <= commit.before7) {
                        world.violation('I14', 'change #' + commit.seq + ' (' + settingKey(commit) + ') is sent again after a status 7 without a reauthentication');
                    }
                });
            }
        };

        // ---- Nest: the observe stream
        let epoch = 0, streamOpen = true, lastMessage = 0;
        const message = (body, traits, snapshot) => {
            if (snapshot) {
                conn.observeStreamSeq++;
            }
            conn.noteContact();
            // What Nest says the settings are, per setting since the last change made to it
            for (const type in body) {
                for (const id in body[type]) {
                    for (const key in body[type][id]) {
                        const said = world.reported[type + '.' + id + '/' + key];
                        if (said) {
                            said.push(copy(body[type][id][key]));
                        }
                    }
                }
            }
            conn.protobufBody = body;
            const fresh = conn.mergeNestWithProtobufData(conn.currentState, body);
            conn.releaseEchoedMergeUpdates(fresh, conn.refreshSnapshotTraits({ traits: traits, hasDeviceInfo: !!snapshot }));
            conn.updateHomeKit(conn.apiResponseToObjectTree(conn.mergePendingUpdates(fresh)));
        };
        const later = (ms, fn) => {
            world.model++;
            setTimeout(() => {
                world.model--;
                fn();
            }, ms);
        };
        const setpointsBody = id => ({ shared: { [id]: reported(nest[id]) } });
        const ecoBody = id => ({ device: { [id]: { eco: { mode: nest[id].eco ? 'manual-eco' : 'schedule' } } } });
        const traitOf = { setpoints: id => ['target_temperature_settings', 'DEVICE_' + id, SETPOINTS], eco: id => ['eco_mode_state', 'DEVICE_' + id, ECO_STATE] };
        // Nest's state of a trait has changed: its message, with the state as it is now
        const changed = (id, trait) => {
            const delay = draw(scenario, 'echo', world.echoes++);
            if (delay === null || !streamOpen) {
                return; // never, or the new stream's first message will carry it
            }
            const body = trait == 'eco' ? ecoBody(id) : setpointsBody(id), from = epoch;
            let due = clock.now + delay;
            if (!profile.lossy) {
                due = lastMessage = Math.max(due, lastMessage + 1);
            }
            later(due - clock.now, () => {
                if (from == epoch) {
                    note('Nest reports ' + id.slice(0, 2) + ' ' + JSON.stringify(body));
                    message(body, [traitOf[trait](id)], false);
                }
            });
        };
        const snapshot = () => {
            const body = { shared: {}, device: {} }, traits = [];
            for (const id of IDS) {
                body.shared[id] = reported(nest[id]);
                body.device[id] = ecoBody(id).device[id];
                traits.push(traitOf.setpoints(id), traitOf.eco(id), ['hvac_control', 'DEVICE_' + id, 'x']);
            }
            note('new stream: ' + JSON.stringify(body.shared));
            message(body, traits, true);
        };
        const restart = () => {
            const from = ++epoch;
            streamOpen = false;
            later(draw(scenario, 'stream', world.restarts++), () => {
                if (from == epoch) {
                    streamOpen = true;
                    snapshot();
                }
            });
        };
        conn.observeRestart = () => {
            note('the plugin restarts the stream');
            restart();
        };

        // ---- Nest: requests
        const decode = bytes => conn.TraitMap.decode(bytes).set.map(el => {
            const type = env.root.lookupType(el.property.type_url.split('/')[1]);
            return { id: el.object.id.replace('DEVICE_', ''), key: el.object.key, value: type.toObject(type.decode(el.property.value), { enums: String, defaults: true }) };
        });
        const apply = request => {
            request.set.forEach(el => {
                const state = nest[el.id];
                if (el.key == 'target_temperature_settings') {
                    Object.assign(state, { mode: el.value.settings.hvacMode, heat: el.value.settings.targetTemperatureHeat.value, cool: el.value.settings.targetTemperatureCool.value, active: el.value.active.value });
                    changed(el.id, 'setpoints');
                } else if (el.key == 'eco_mode_state') {
                    state.eco = el.value.ecoEnabled != 'OFF';
                    changed(el.id, 'eco');
                }
            });
            note('Nest applies request ' + request.number);
        };
        world.request = config => new Promise((resolve, reject) => {
            const number = world.requests.length + 1;
            const answer = draw(scenario, 'answer', number - 1);
            const request = { number: number, sent: clock.now, answer: answer.kind, bytes: Buffer.isBuffer(config.data) ? config.data.toString('hex') : String(config.data), carries: [], set: [] };
            world.requests.push(request);
            world.open++;
            const from = Buffer.isBuffer(config.data) && built.get(config.data);
            if (!from || !from.entries.length) {
                world.violation('I1', 'request ' + number + ' was sent with no change to carry');
            } else {
                request.set = decode(config.data);
                send(request, from);
            }
            note('request ' + number + ' sent: ' + request.set.map(el => el.id.slice(0, 2) + ' ' + (el.value.settings ? el.value.settings.hvacMode + ' ' + el.value.settings.targetTemperatureHeat.value.toFixed(1) + '/' + el.value.settings.targetTemperatureCool.value.toFixed(1) : 'eco ' + el.value.ecoEnabled)).join(', ') +
                ' carrying ' + request.carries.map(commit => '#' + commit.seq).join(',') + ' -> ' + answer.kind + ' after ' + answer.ms / 1000 + ' s' + (answer.applied ? ', applied' : ''));
            if (answer.applied) {
                later(Math.round(answer.ms * answer.share), () => apply(request));
            }
            later(answer.ms, () => {
                world.open--;
                request.answered = clock.now;
                answered(request, answer);
                note('request ' + number + ' answered: ' + answer.kind);
                if (ANSWERED.includes(answer.kind)) {
                    resolve({ data: Buffer.from(answer.body, 'hex'), status: 200, statusText: 'OK', headers: {}, config: config });
                } else {
                    const error = new Error(answer.kind);
                    if (/^\d+$/.test(answer.kind)) {
                        error.response = { status: Number(answer.kind), headers: {}, data: Buffer.alloc(0) };
                    } else {
                        error.code = answer.kind == 'timeout' ? 'ECONNABORTED' : answer.kind;
                    }
                    error.config = config;
                    reject(error);
                }
            });
        });
        // I1, I2: checked as each request goes out
        const send = (request, from) => {
            // I13. The request's bytes hold the values of the changes it is taken to carry (a check of this test's
            // own bookkeeping as much as of the plugin: where it can be told, a thermostat in range mode).
            for (const entry of from.entries) {
                const el = request.set.find(el => 'shared.' + el.id == entry.object && el.value.settings && el.value.settings.hvacMode == 'RANGE');
                const wire = el && (entry.key == LOW ? el.value.settings.targetTemperatureHeat.value : entry.key == HIGH ? el.value.settings.targetTemperatureCool.value : undefined);
                if (wire !== undefined && !same(wire, entry.value)) {
                    // (P13: the same where the threshold was written while the accessory held a single mode and the
                    // request is built for range mode: pushUpdates turns it into a target temperature, which a
                    // range-mode request does not carry. It happens after a mode change was dropped by a failed
                    // push; the threshold is then not sent at all. So it was before.)
                    world.violation(entry.mode == 'range' ? 'I13' : 'P13', 'request ' + request.number + ' is taken to carry ' + entry.object + '/' + entry.key + '=' + entry.value + ', and its bytes hold ' + wire);
                }
            }
            for (const entry of from.entries) {
                const commit = commitOf(entry, from.commits);
                if (!commit) {
                    world.violation('I1', 'request ' + request.number + ' carries ' + entry.object + ' ' + entry.key + '=' + JSON.stringify(entry.value) + ', which no change asked for');
                    continue;
                }
                if (request.carries.includes(commit)) {
                    continue; // (queued twice with the same value)
                }
                request.carries.push(commit);
                const group = commit.group;
                group.sends++;
                commit.flying++;
                // I1. A change is sent once; once more for each request carrying it that failed in a way the plugin
                // retries; and at most RESEND_MAX times more because Nest answered a request carrying it with the
                // bare status 4.
                if (group.sends > group.changes + group.errors + Math.min(group.lost, RESEND_MAX * group.changes)) {
                    world.violation('I1', 'request ' + request.number + ' is send ' + group.sends + ' of change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + (group.changes > 1 ? ', made ' + group.changes + ' times' : '') + '), after ' + group.errors + ' failed request(s) and ' + group.lost + ' status 4 or 7');
                }
                // I2. Never an older value of a setting after a newer one has been sent. (P2: the same, where the
                // request is not one sent again after a status 4 but the plugin's retry of a failed request, or a
                // push it had delayed. Not this test's subject: the version before does the same.)
                const last = world.sent[settingKey(commit)];
                const again = commit.resent;
                commit.resent = false;
                commit.resentIn = again ? request.number : commit.resentIn;
                if (last && last.seq > commit.seq && same(last.value, commit.value)) {
                    // The newer change asked for the same value (to what Nest can tell): nothing older about it
                } else if (last && last.seq > commit.seq) {
                    world.violation(again ? 'I2' : 'P2', 'request ' + request.number + ' carries ' + settingKey(commit) + '=' + JSON.stringify(commit.value) + ' (change #' + commit.seq + (again ? ', sent again after a status 4' : ', last answered ' + commit.last) + ') after request ' + last.request + ' carried the newer ' + JSON.stringify(last.value) + ' (change #' + last.seq + ')');
                } else {
                    world.sent[settingKey(commit)] = { seq: commit.seq, value: commit.value, request: request.number };
                }
            }
        };
        const answered = (request, answer) => {
            const off = fs.existsSync(flagPath);
            for (const commit of request.carries) {
                commit.flying--;
                commit.last = answer.kind;
                // (For I15: a resend answered as taken, nothing having been settled since it was decided but, then,
                // the rest of its own push)
                if (commit.resentIn == request.number && ANSWERED.includes(answer.kind) && !LOST.includes(answer.kind) && world.settles == commit.resentAt + 1) {
                    world.taken.push({ commit: commit, at: clock.now });
                }
                if (LOST.includes(answer.kind) && !off && options.status4Lost !== false) {
                    commit.group.lost++;
                    // (For I14: how many reauthentications had succeeded when a status 7 came)
                    commit.before7 = answer.kind == 'status7' ? world.reauthenticated : undefined;
                } else if (ANSWERED.includes(answer.kind)) {
                    commit.accepted = true;
                } else if (answer.kind == '401' || answer.kind == '403' || RETRYABLE_CODES.includes(answer.kind == 'timeout' ? 'ECONNABORTED' : answer.kind)) {
                    commit.group.errors++;
                }
            }
            world.failed = world.failed || !ANSWERED.includes(answer.kind);
        };

        // ---- HomeKit
        const shown = () => {
            const state = conn.mergePendingUpdates(conn.currentState);
            const out = {};
            for (const id of IDS) {
                out[id] = { type: state.shared[id][TYPE], low: state.shared[id][LOW], high: state.shared[id][HIGH], eco: state.device[id].eco.mode };
            }
            return out;
        };
        const characteristic = (id, type) => accessories[id].getService(hap.Service.Thermostat).getCharacteristic(type);
        const write = (target, value) => target.handleSetRequest(value, undefined).then(() => {}, () => {
            world.rejected++;
        });
        const C = hap.Characteristic;
        const act = async op => {
            note('op ' + JSON.stringify(op));
            if (op.kind == 'pair' && shown()[op.id].type != 'range') {
                // In a single mode a controller writes one temperature. (Of two thresholds written together there,
                // pushUpdates sends one, as the target temperature, and the other is held until Nest's state is read
                // 300 s later: so it was before, and it is not this test's subject.)
                write(characteristic(op.id, op.at % 2 ? C.HeatingThresholdTemperature : C.CoolingThresholdTemperature), op.at % 2 ? op.low : op.high);
            } else if (op.kind == 'pair') {
                write(characteristic(op.id, C.HeatingThresholdTemperature), op.low);
                if (op.gap) {
                    await clock.runTo(clock.now + op.gap, check);
                }
                write(characteristic(op.id, C.CoolingThresholdTemperature), op.high);
            } else if (op.kind == 'one') {
                write(characteristic(op.id, op.low === undefined ? C.CoolingThresholdTemperature : C.HeatingThresholdTemperature), op.low === undefined ? op.high : op.low);
            } else if (op.kind == 'mode') {
                write(characteristic(op.id, C.TargetHeatingCoolingState), { heat: C.TargetHeatingCoolingState.HEAT, cool: C.TargetHeatingCoolingState.COOL, range: C.TargetHeatingCoolingState.AUTO }[op.mode]);
            } else if (op.kind == 'eco') {
                write(accessories[op.id].getService('Eco Mode').getCharacteristic(C.On), op.on);
            } else if (op.kind == 'temperature') {
                write(characteristic(op.id, C.TargetTemperature), op.value);
            } else if (op.kind == 'outside') {
                const state = nest[op.id];
                Object.assign(state, op.low === undefined ? {} : { heat: f32(op.low) }, op.high === undefined ? {} : { cool: f32(op.high) }, op.mode === undefined ? {} : { mode: op.mode.toUpperCase() });
                changed(op.id, 'setpoints');
            } else if (op.kind == 'message') {
                if (streamOpen) {
                    message({ shared: { [op.id]: { current_temperature: 21 + (op.at % 7) / 10 } } }, [['temperature', 'DEVICE_' + op.id, 'x']], false);
                }
            } else if (op.kind == 'stream') {
                restart();
            } else if (op.kind == 'flag') {
                setFlag(!fs.existsSync(flagPath));
            }
            await clock.settle();
        };

        // ---- What must hold at every moment
        const check = () => {
            const now = clock.now;
            // I9. The held changes are well formed: each writes something, none holds longer than its limit.
            for (const update of conn.mergeUpdates) {
                const label = update.object.object_key + ' ' + JSON.stringify(update.object.value);
                if (!Object.keys(update.object.value).length) {
                    world.violation('I9', 'a held change writes nothing: ' + label);
                }
                if (update.expiry_time - now > (update.pushed ? ECHO_MAX_MS : UNPUSHED_MAX_MS)) {
                    world.violation('I9', 'a held change holds for ' + (update.expiry_time - now) / 1000 + ' s: ' + label + (update.pushed ? ' (pushed)' : ' (waiting)'));
                }
            }
            for (const el of conn.pendingUpdates) {
                if (!Object.keys(el.object.value).length) {
                    world.violation('I9', 'a queued change writes nothing: ' + el.object.object_key);
                }
            }
            // I8. While a request carrying the last change made to a setting is unanswered, HomeKit is shown that
            // change's value, or a value Nest has reported for the setting since the change was made.
            let state = null;
            const showing = commit => {
                state = state || conn.mergePendingUpdates(conn.currentState);
                const [type, id] = commit.object.split('.');
                return state[type][id][commit.key];
            };
            const said = (commit, value) => world.reported[settingKey(commit)].some(el => same(el, value));
            for (const key in world.latest) {
                const commit = world.latest[key];
                if (commit.flying > 0 && !same(showing(commit), commit.value) && !said(commit, showing(commit))) {
                    // (P8: the same once a request has failed. A failed push drops every change made before it
                    // started, also those other requests are still carrying, and a change waiting through a chain
                    // of retries is held for 120 s only: so it was before.)
                    world.violation(world.failed ? 'P8' : 'I8', 'change #' + commit.seq + ' (' + key + '=' + JSON.stringify(commit.value) + ') is on its way to Nest, but HomeKit is shown ' + JSON.stringify(showing(commit)));
                }
            }
            // I15. A change sent again was waiting to be pushed until its answer, and its 300 s echo wait counts from
            // that answer. (Where no other push has settled anything since the status 4 or 7: an answered push
            // settles every change made before it started, also one that another push is about to send again.)
            for (const { commit, at } of world.taken.splice(0)) {
                const update = conn.mergeUpdates.find(update => update.object.object_key == commit.object && exact(update.object.value[commit.key], commit.value));
                if (update && typeof commit.value == 'number' && (!update.pushed || update.expiry_time != at + ECHO_MAX_MS)) {
                    world.violation('I15', 'change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + '), sent again and taken, is ' + (update.pushed ? 'held for ' + (update.expiry_time - now) / 1000 + ' s' : 'still waiting to be pushed'));
                }
            }
            // I10. Once the log has said a change was NOT applied, it is no longer held. (P10: the same after a line
            // of the plugin's older paths.)
            for (const { commit, resend, named } of world.dropped.splice(0)) {
                if (typeof commit.value == 'number' && conn.mergeUpdates.some(update => update.expiry_time > now && update.object.object_key == commit.object && exact(update.object.value[commit.key], commit.value))) {
                    if (named) {
                        world.violation(resend ? 'I10' : 'P10', 'change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + ') was said to be NOT applied, and is still held');
                    } else {
                        // P7, counted and not a failure: the line names a threshold as the target temperature it
                        // was sent as, and the threshold stays held (and shown) until Nest's state is read, up to
                        // 300 s later. So a threshold written in a single mode is held whatever becomes of it.
                        world.violation('P7', 'change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + ') was said to be NOT applied as a target temperature, and is still held');
                    }
                }
            }
            // (For I3: whether Nest has had the value of the last change made to each setting since a request
            // carrying it went out)
            for (const key in world.latest) {
                const commit = world.latest[key], id = commit.object.split('.')[1];
                const has = commit.key == 'eco' ? { mode: nest[id].eco ? 'manual-eco' : 'schedule' } : reported(nest[id])[commit.key];
                commit.reported = commit.reported || (commit.group.sends > 0 && same(has, commit.value));
            }
            // I11. The accessories hold what the plugin shows: Nest's state as last reported, with the held changes.
            // (Not in the 10 ms between a held change's end and the timer that acts on it.)
            for (const id of conn.mergeUpdates.some(update => update.expiry_time <= now) ? [] : IDS) {
                for (const key of [TYPE, LOW, HIGH]) {
                    state = state || conn.mergePendingUpdates(conn.currentState);
                    if (!same(accessories[id].device[key], state.shared[id][key])) {
                        world.violation('I11', id.slice(0, 2) + ' ' + key + ': the accessory holds ' + JSON.stringify(accessories[id].device[key]) + ', the plugin would show ' + JSON.stringify(state.shared[id][key]));
                    }
                }
            }
        };
        const step = label => {
            check();
            world.steps.push({
                at: at(), label: label, shown: shown(),
                held: conn.mergeUpdates.map(update => [update.object.object_key, copy(update.object.value), !!update.pushed, !!update.refreshing, update.expiry_time - clock.now]),
                queued: conn.pendingUpdates.map(el => [el.object.object_key, copy(el.object.value)]),
                accessories: IDS.map(id => [accessories[id].device[TYPE], accessories[id].device[LOW], accessories[id].device[HIGH], accessories[id].device.hvac_mode]),
                requests: world.requests.length, restarts: world.restarts
            });
        };

        // ---- Run the operations, then let everything come to rest
        try {
            for (const op of scenario.ops) {
                await clock.runTo(start + op.at, check);
                await act(op);
                step(op.kind);
            }
            await settle();
        } catch (error) {
            world.violation('I7', 'exception: ' + (error && error.stack || error));
        }

        async function settle() {
            // At rest: for a minute, no request unanswered, nothing to come from Nest, no change queued or held, and no
            // timer set but the ping's (a push may be waiting out a delay)
            const busy = () => world.open > 0 || world.model > 0 || conn.pendingUpdates.length > 0 || conn.mergeUpdates.length > 0 || clock.timers.size > 1;
            const limit = clock.now + 45 * 60000;
            for (let quiet = 0; quiet < 12 && clock.now < limit; quiet = busy() ? 0 : quiet + 1) {
                await clock.runTo(clock.now + 5000, check);
            }
            step('at rest');
            // I5. Nothing is left: no request unanswered, no change queued or held, no timer but the ping's.
            if (world.open || conn.pendingUpdates.length || conn.mergeUpdates.length || conn.mergeEndTimer || conn.echoRefresh) {
                world.violation('I5', 'not at rest after 45 minutes: ' + world.open + ' request(s) unanswered, ' + conn.pendingUpdates.length + ' change(s) queued, ' + conn.mergeUpdates.length + ' held' + (conn.mergeEndTimer ? ', the merge timer set' : '') + (conn.echoRefresh ? ', an echo refresh under way' : ''));
            }
            if (clock.timers.size != 1) {
                world.violation('I5', (clock.timers.size - 1) + ' timer(s) left: ' + Array.from(clock.timers.values()).filter(timer => !timer.interval).map(timer => 'due in ' + (timer.at - clock.now) / 1000 + ' s, ' + String(timer.fn).replace(/\s+/g, ' ').slice(0, 80)).join('; '));
            }
            // I3. No change is lost silently: each was taken by Nest (a request carrying it was answered, and not
            // with the bare status 4, unless that is switched off), or Nest has had its value at some moment since a
            // request carrying it went out (it went in another request too: every request for a thermostat's
            // setpoints carries its mode and both setpoints; or Nest had the value already), or it was replaced by
            // a newer change to the same setting, or the log says it was NOT applied.
            for (const commit of world.commits) {
                if (!commit.accepted && !commit.dropped && !commit.reported && world.latest[settingKey(commit)] === commit) {
                    // (P3: the same where the last request carrying the change failed, and not with a status 4, or
                    // where it never went out because a failed push, queued again in front of it, wrote over it: the
                    // plugin's older paths. In a single mode a failed push is queued again as it was sent, as one
                    // target temperature, and a second threshold written with it is never sent.)
                    world.violation((commit.last && !LOST.includes(commit.last)) || commit.overridden ? 'P3' : 'I3', 'change #' + commit.seq + ' (' + settingKey(commit) + '=' + JSON.stringify(commit.value) + ', made at ' + (commit.at - start) / 1000 + ' s) was sent ' + commit.group.sends + ' time(s), never taken by Nest, and nothing says it was lost');
                }
            }
            // I2, at rest: the last value sent for a setting is the last one asked for, unless that was lost.
            for (const key in world.latest) {
                const commit = world.latest[key], last = world.sent[key];
                if (!commit.dropped && (!last || (last.seq != commit.seq && !same(last.value, commit.value)))) {
                    world.violation(world.violations.some(el => el.invariant == 'P2') || commit.overridden ? 'P2' : 'I2', 'the last change to ' + key + ' (#' + commit.seq + ', ' + JSON.stringify(commit.value) + ') is not the last value sent for it (' + (last ? '#' + last.seq + ', ' + JSON.stringify(last.value) : 'none') + ')');
                }
            }
            // I4. HomeKit shows Nest's state. When messages can be lost, only after a new stream has delivered it.
            if (profile.lossy) {
                restart();
                await clock.runTo(clock.now + 180000, check);
                step('after a new stream');
            }
            const hk = shown();
            for (const id of IDS) {
                const want = Object.assign(reported(nest[id]), { eco: nest[id].eco ? 'manual-eco' : 'schedule' });
                const device = accessories[id].device;
                const got = { [TYPE]: hk[id].type, [LOW]: hk[id].low, [HIGH]: hk[id].high, eco: hk[id].eco };
                const accessory = { [TYPE]: device[TYPE], [LOW]: device[LOW], [HIGH]: device[HIGH], eco: device.eco.mode };
                for (const key of [TYPE, LOW, HIGH, 'eco']) {
                    if (!same(got[key], want[key]) || !same(accessory[key], want[key])) {
                        world.violation('I4', id.slice(0, 2) + ' ' + key + ': Nest has ' + JSON.stringify(want[key]) + ', the plugin shows ' + JSON.stringify(got[key]) + ', the accessory holds ' + JSON.stringify(accessory[key]));
                    }
                }
            }
        }

        clock.timers.clear();
        env.world = null;
        const skip = line => !/not sending it again: nest-no-resend\.on exists/.test(line);
        return {
            violations: world.violations,
            requests: world.requests.length,
            status4: world.requests.filter(request => request.answer == 'status4').length,
            status7: world.requests.filter(request => request.answer == 'status7').length,
            // How often each thing the test is about happened
            happened: {
                resent: world.kept.filter(line => / - sending .* again/.test(line)).length,
                givenUp: world.kept.filter(line => /when it was sent again .* NOT applied/.test(line)).length,
                notResent: world.kept.filter(line => /not sending it again: a newer change/.test(line)).length,
                switchedOff: world.kept.filter(line => /not sending it again: nest-no-resend/.test(line)).length,
                lost: world.kept.filter(line => /NOT applied/.test(line)).length,
                reauthFailed: world.kept.filter(line => /the reauthentication failed/.test(line)).length,
                status7: world.requests.filter(request => request.answer == 'status7').length,
                refreshes: world.kept.filter(line => /has not echoed/.test(line)).length,
                rejectedWrites: world.rejected
            },
            commits: world.commits.length,
            digest: crypto.createHash('sha1').update(JSON.stringify([world.requests.map(request => [request.sent, request.answered, request.bytes]), world.steps, world.kept.filter(skip)])).digest('hex'),
            trace: options.trace ? { events: world.events, steps: world.steps, requests: world.requests.map(request => ({ number: request.number, sent: (request.sent - start) / 1000, answered: (request.answered - start) / 1000, answer: request.answer, bytes: request.bytes })), kept: world.kept.filter(skip) } : null
        };
    }

    return { prepare: prepare, run: run, dispose: dispose };
}

// Cuts a scenario that breaks `invariant` down: operations are removed, and answers made normal, for as long as it
// still breaks it. fails(scenario) -> whether it does.
async function shrink(scenario, fails) {
    let best = copy(scenario);
    for (let changed = true; changed;) {
        changed = false;
        for (let i = best.ops.length - 1; i >= 0; i--) {
            const trial = copy(best);
            trial.ops.splice(i, 1);
            if (await fails(trial)) {
                best = trial;
                changed = true;
            }
        }
        for (const index of Object.keys(best.draws.answer || {})) {
            const answer = best.draws.answer[index];
            if (answer.kind != 'ok' || answer.ms != 1000) {
                const trial = copy(best);
                trial.draws.answer[index] = { kind: 'ok', ms: 1000, applied: true, share: 1, body: '' };
                if (await fails(trial)) {
                    best = trial;
                    changed = true;
                }
            }
        }
    }
    return best;
}

module.exports = { create: create, generate: generate, shrink: shrink, NOT_APPLIED: NOT_APPLIED };
