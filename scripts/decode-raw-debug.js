#!/usr/bin/env node
'use strict';

// Reads the nest-raw-debug.jsonl written by lib/raw-debug.js.
//
//   node scripts/decode-raw-debug.js [--summary] [--resource SUBSTR] [--key SUBSTR] [--kind frame|trait|set] FILE
//
// Default: one block per line with the decoded value, a generic wire-format decode of the raw trait bytes (field
// numbers, varints, fixed32 as float, nested messages) and the fields the plugin's .proto schema does not declare.
// --summary: per resource and trait, the message count, inter-arrival times, re-sends of unchanged bytes, distinct
// decoded values and undeclared field paths; and the cadence of the observe frames themselves.

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const protobuf = require('protobufjs');
const { decodeGeneric, formatGeneric, undeclaredFields } = require('../lib/raw-debug.js');

protobuf.util.Long = null;
protobuf.configure();

function parseArgs(argv) {
    const args = { summary: false, resource: null, key: null, kind: null, file: null };
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === '--summary') {
            args.summary = true;
        } else if (argv[i] === '--resource' || argv[i] === '--key' || argv[i] === '--kind') {
            args[argv[i].substr(2)] = argv[++i];
        } else if (argv[i] === '-h' || argv[i] === '--help') {
            args.help = true;
        } else {
            args.file = argv[i];
        }
    }
    return args;
}

function lookupType(root, typeUrl) {
    try {
        return typeUrl ? root.lookupType(typeUrl.split('/').pop()) : null;
    } catch(error) {
        return null;
    }
}

function undeclaredPaths(root, typeUrl, raw) {
    const type = lookupType(root, typeUrl);
    if (!type) {
        return [{ path: '(type not in schema: ' + typeUrl + ')' }];
    }
    return undeclaredFields(Buffer.from(raw, 'base64'), type);
}

function describe(entry) {
    if (entry.truncated) {
        return entry.path + ' (wt2, ' + entry.len + ' bytes, not logged)';
    }
    if (entry.error) {
        return entry.path + ' ' + entry.error;
    }
    return entry.path + ' = ' + (entry.field ? formatGeneric([entry.field]) : '?');
}

function stats(values) {
    if (!values.length) {
        return 'n/a';
    }
    const sorted = values.slice().sort((a, b) => a - b);
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const pick = q => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    const f = x => x < 10 ? x.toFixed(1) : Math.round(x).toString();
    return 'min ' + f(sorted[0]) + ' p50 ' + f(pick(0.5)) + ' mean ' + f(mean) + ' p90 ' + f(pick(0.9)) + ' max ' + f(sorted[sorted.length - 1]) + ' s';
}

function matches(rec, args) {
    if (args.kind && rec.kind !== args.kind) {
        return false;
    }
    if (args.resource && (rec.kind === 'frame' || (rec.resource || '').indexOf(args.resource) < 0)) {
        return false;
    }
    if (args.key && (rec.kind !== 'trait' || (rec.key || '').indexOf(args.key) < 0)) {
        return false;
    }
    return true;
}

function dump(rec, root) {
    if (rec.kind === 'frame') {
        let text = rec.t + ' stream ' + rec.stream + ' #' + rec.seq + ' FRAME ' + rec.bytes + ' bytes';
        if (rec.noop) {
            text += ', noop x' + rec.noop;
        }
        (rec.messages || []).forEach((m, i) => {
            text += ', message[' + i + '] gets ' + m.gets + ' sets ' + m.sets;
            (m.extra || []).forEach(e => {
                text += '\n    ' + describe(e);
            });
        });
        (rec.status || []).forEach(s => {
            text += '\n    status: ' + formatGeneric([s]);
        });
        (rec.extra || []).forEach(e => {
            text += '\n    ' + describe(e);
        });
        if (rec.parse_error) {
            text += '\n    parse error: ' + rec.parse_error;
        }
        console.log(text);
        return;
    }
    if (rec.kind === 'set') {
        console.log(rec.t + ' stream ' + rec.stream + ' #' + rec.seq + ' SET ' + rec.resource + ' (' + rec.device_type + ')\n    wire: ' + (rec.generic ? formatGeneric(rec.generic) : 'b64 ' + rec.raw));
        return;
    }
    let text = rec.t + ' stream ' + rec.stream + ' #' + rec.seq + ' ' + rec.resource + ' (' + rec.device_type + ') ' + rec.key + ' [' + rec.type_url + ']';
    text += '\n    value: ' + JSON.stringify(rec.value);
    if (rec.raw !== undefined) {
        try {
            text += '\n    wire: ' + formatGeneric(decodeGeneric(Buffer.from(rec.raw, 'base64')));
        } catch(error) {
            text += '\n    wire: unparseable (' + error.message + ') b64 ' + rec.raw;
        }
        undeclaredPaths(root, rec.type_url, rec.raw).forEach(e => {
            text += '\n    undeclared in trait: ' + describe(e);
        });
    }
    (rec.extra || []).forEach(e => {
        text += '\n    undeclared in envelope: ' + describe(e);
    });
    console.log(text);
}

function newGroup() {
    return { n: 0, times: [], gaps: [], streams: {}, snapshots: 0, resends: 0, lastRaw: null, values: new Map(), paths: new Map() };
}

function countPath(map, entry, prefix) {
    const name = prefix + entry.path + (entry.wt !== undefined ? ' (wt' + entry.wt + ')' : '');
    const seen = map.get(name) || { n: 0, sample: describe(entry) };
    seen.n++;
    map.set(name, seen);
}

function addToGroup(group, rec, ms) {
    group.n++;
    const firstInStream = !group.streams[rec.stream];
    group.streams[rec.stream] = (group.streams[rec.stream] || 0) + 1;
    if (firstInStream) {
        group.snapshots++;
    } else if (group.times.length) {
        group.gaps.push((ms - group.times[group.times.length - 1]) / 1000);
    }
    if (!firstInStream && rec.raw !== undefined && rec.raw === group.lastRaw) {
        group.resends++;
    }
    group.lastRaw = rec.raw;
    group.times.push(ms);
}

function summarize(records, root) {
    const frames = { all: newGroup(), data: newGroup(), noop: newGroup(), status: newGroup() };
    const frameExtra = new Map();
    const resources = new Map();

    records.forEach(rec => {
        const ms = Date.parse(rec.t);
        if (rec.kind === 'frame') {
            const type = rec.status ? 'status' : (rec.messages && rec.messages.length ? 'data' : (rec.noop ? 'noop' : 'data'));
            addToGroup(frames.all, rec, ms);
            addToGroup(frames[type], rec, ms);
            (rec.extra || []).forEach(e => countPath(frameExtra, e, 'stream body '));
            (rec.messages || []).forEach(m => (m.extra || []).forEach(e => countPath(frameExtra, e, '')));
            return;
        }
        if (rec.kind !== 'trait') {
            return;
        }
        if (!resources.has(rec.resource)) {
            resources.set(rec.resource, { device_type: rec.device_type, resource_type: rec.resource_type, keys: new Map() });
        }
        const resource = resources.get(rec.resource);
        if (!resource.keys.has(rec.key)) {
            resource.keys.set(rec.key, newGroup());
        }
        const group = resource.keys.get(rec.key);
        addToGroup(group, rec, ms);
        const value = JSON.stringify(rec.value);
        group.values.set(value, (group.values.get(value) || 0) + 1);
        if (rec.raw !== undefined) {
            undeclaredPaths(root, rec.type_url, rec.raw).forEach(e => countPath(group.paths, e, 'trait '));
        }
        (rec.extra || []).forEach(e => countPath(group.paths, e, 'envelope '));
    });

    const span = records.length ? (Date.parse(records[records.length - 1].t) - Date.parse(records[0].t)) / 1000 : 0;
    console.log('Observe frames: ' + frames.all.n + ' over ' + (span / 3600).toFixed(2) + ' h, ' + Object.keys(frames.all.streams).length + ' stream(s)');
    ['data', 'noop', 'status'].forEach(type => {
        if (frames[type].n) {
            console.log('  ' + type + ': ' + frames[type].n + ', gap within a stream ' + stats(frames[type].gaps));
        }
    });
    frameExtra.forEach((seen, name) => console.log('  undeclared ' + name + ' x' + seen.n + ', e.g. ' + seen.sample));

    resources.forEach((resource, id) => {
        console.log('\n' + id + ' (' + resource.device_type + ', ' + resource.resource_type + ')');
        resource.keys.forEach((group, key) => {
            console.log('  ' + key + ': ' + group.n + ' msgs (' + group.snapshots + ' first-in-stream), ' + group.resends + ' re-sends of unchanged bytes, ' + group.values.size + ' distinct value(s)');
            console.log('    gap within a stream: ' + stats(group.gaps));
            const values = Array.from(group.values.entries()).sort((a, b) => b[1] - a[1]);
            values.slice(0, 8).forEach(([value, n]) => console.log('    x' + n + ' ' + (value.length > 160 ? value.substr(0, 160) + '...' : value)));
            if (values.length > 8) {
                console.log('    ... ' + (values.length - 8) + ' more');
            }
            group.paths.forEach((seen, name) => console.log('    undeclared ' + name + ' x' + seen.n + ', e.g. ' + seen.sample));
        });
    });
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help || !args.file) {
        console.log('usage: node scripts/decode-raw-debug.js [--summary] [--resource SUBSTR] [--key SUBSTR] [--kind frame|trait|set] nest-raw-debug.jsonl');
        process.exit(args.help ? 0 : 2);
    }
    const root = await protobuf.load(path.join(__dirname, '..', 'lib', 'protobuf', 'root.proto'));
    root.resolveAll();

    const records = [];
    const input = readline.createInterface({ input: fs.createReadStream(args.file), crlfDelay: Infinity });
    let lineNo = 0;
    for await (const line of input) {
        lineNo++;
        if (!line.trim()) {
            continue;
        }
        let rec;
        try {
            rec = JSON.parse(line);
        } catch(error) {
            console.error('line ' + lineNo + ': not JSON (' + error.message + ')');
            continue;
        }
        if (!matches(rec, args)) {
            continue;
        }
        if (args.summary) {
            records.push(rec);
        } else {
            dump(rec, root);
        }
    }
    if (args.summary) {
        summarize(records, root);
    }
}

main().catch(error => {
    console.error(error);
    process.exit(1);
});
