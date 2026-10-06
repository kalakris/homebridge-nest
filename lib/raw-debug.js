'use strict';

// Passive diagnostic logger for the protobuf observe stream, plus a generic protobuf wire-format decoder.
//
// Logging is on only while <storagePath>/nest-raw-debug.on exists (checked at most every 30 s). It writes JSON lines
// to <storagePath>/nest-raw-debug.jsonl: one per observe frame, and one per trait of each Nest Temperature Sensor
// (all traits) and each thermostat (temperature, humidity, HVAC and comfort-sensing traits only), carrying the raw
// trait bytes and every envelope field the plugin's schema does not declare (protobufjs drops those when decoding).
// It never sends anything to Nest and never throws into the caller. scripts/decode-raw-debug.js reads the output.

const fs = require('fs');
const path = require('path');

const FLAG_FILE = 'nest-raw-debug.on';
const OUTPUT_FILE = 'nest-raw-debug.jsonl';
const FLAG_CHECK_INTERVAL_MS = 30 * 1000;
const DEFAULT_MAX_BYTES = 50 * 1024 * 1024;
// Undeclared envelope fields longer than this are logged by length only (the initial snapshot carries account data)
const MAX_EXTRA_FIELD_BYTES = 2048;
const MAX_DEPTH = 12;

const THERMOSTAT_RESOURCE_TYPES = ['nest.resource.NestLearningThermostat3Resource', 'nest.resource.NestAgateDisplayResource', 'nest.resource.NestOnyxResource', 'google.resource.GoogleZirconium1Resource', 'nest.resource.NestLearningThermostat3v2Resource', 'nest.resource.NestThermostat3Resource', 'nest.resource.NestAmber2DisplayResource'];
const THERMOSTAT_TRAIT_PATTERN = /temperature|humidity|hvac|comfort/i;

// ---- Generic wire-format decoding ----

function bitsToHex(bits) {
    while (bits.length % 4) {
        bits = '0' + bits;
    }
    let hex = '';
    for (let i = 0; i < bits.length; i += 4) {
        hex += parseInt(bits.substr(i, 4), 2).toString(16);
    }
    return hex.replace(/^0+(?=.)/, '');
}

// Returns { value, next } and, when value exceeds 2^53, hex (exact; value is then approximate)
function readVarint(buf, pos) {
    let value = 0, mul = 1, i = pos;
    const groups = [];
    for (;;) {
        if (i >= buf.length) {
            throw new Error('truncated varint at ' + pos);
        }
        const b = buf[i++];
        groups.push(b & 0x7f);
        value += (b & 0x7f) * mul;
        mul *= 128;
        if (!(b & 0x80)) {
            break;
        }
        if (groups.length >= 10) {
            throw new Error('varint too long at ' + pos);
        }
    }
    const result = { value: value, next: i };
    if (value > Number.MAX_SAFE_INTEGER) {
        result.hex = bitsToHex(groups.reverse().map(g => g.toString(2).padStart(7, '0')).join(''));
    }
    return result;
}

// Strict parse of one message level. Returns [{ f, wt, value | hex | buf }]; throws if buf is not a valid message.
function parseFields(buf) {
    const fields = [];
    let pos = 0;
    while (pos < buf.length) {
        const tag = readVarint(buf, pos);
        pos = tag.next;
        const f = Math.floor(tag.value / 8), wt = tag.value % 8;
        if (f < 1 || f > 536870911) {
            throw new Error('bad field number ' + f);
        }
        if (wt === 0) {
            const v = readVarint(buf, pos);
            pos = v.next;
            fields.push(v.hex ? { f: f, wt: 0, value: v.value, hex: v.hex } : { f: f, wt: 0, value: v.value });
        } else if (wt === 1 || wt === 5) {
            const n = wt === 1 ? 8 : 4;
            if (pos + n > buf.length) {
                throw new Error('truncated fixed field ' + f);
            }
            fields.push({ f: f, wt: wt, buf: buf.slice(pos, pos + n) });
            pos += n;
        } else if (wt === 2) {
            const len = readVarint(buf, pos);
            pos = len.next;
            if (pos + len.value > buf.length) {
                throw new Error('truncated length-delimited field ' + f);
            }
            fields.push({ f: f, wt: 2, buf: buf.slice(pos, pos + len.value) });
            pos += len.value;
        } else {
            throw new Error('unsupported wire type ' + wt + ' (field ' + f + ')');
        }
    }
    return fields;
}

function isPrintable(buf) {
    const s = buf.toString('utf8');
    if (Buffer.byteLength(s) !== buf.length || s.indexOf('�') >= 0) {
        return false;
    }
    return /^[^\x00-\x08\x0b\x0c\x0e-\x1f\x7f]*$/.test(s); // eslint-disable-line no-control-regex
}

function roundFloat(x) {
    return Number.isFinite(x) ? Number(x.toPrecision(9)) : String(x);
}

// Describes one parsed field: varint -> v (+ hex, + zigzag s), fixed32 -> u32 + f32, fixed64 -> hex + f64,
// length-delimited -> str if printable text, else sub (recursive) if it parses as a message, else b64.
function describeField(field, depth) {
    const out = { f: field.f, wt: field.wt };
    if (field.wt === 0) {
        out.v = field.value;
        if (field.hex) {
            out.hex = field.hex;
        }
    } else if (field.wt === 5) {
        out.u32 = field.buf.readUInt32LE(0);
        out.f32 = roundFloat(field.buf.readFloatLE(0));
    } else if (field.wt === 1) {
        out.hex = Buffer.from(field.buf).reverse().toString('hex');
        out.f64 = roundFloat(field.buf.readDoubleLE(0));
    } else {
        out.len = field.buf.length;
        if (field.buf.length === 0) {
            out.b64 = '';
        } else if (isPrintable(field.buf)) {
            out.str = field.buf.toString('utf8');
        } else {
            let sub = null;
            if ((depth || 0) < MAX_DEPTH) {
                try {
                    sub = decodeGeneric(field.buf, (depth || 0) + 1);
                } catch(error) {
                    sub = null;
                }
            }
            if (sub) {
                out.sub = sub;
            } else {
                out.b64 = field.buf.toString('base64');
            }
        }
    }
    return out;
}

function decodeGeneric(buf, depth) {
    return parseFields(buf).map(field => describeField(field, depth || 0));
}

// Compact one-line rendering of decodeGeneric output, e.g. 1{1{1:f32=20.37}} 2:v=1
function formatGeneric(fields) {
    return fields.map(d => {
        if (d.sub) {
            return d.f + '{' + formatGeneric(d.sub) + '}';
        } else if (d.wt === 0) {
            return d.f + ':v=' + (d.hex ? '0x' + d.hex : d.v);
        } else if (d.wt === 5) {
            return d.f + ':f32=' + d.f32 + '(u32=' + d.u32 + ')';
        } else if (d.wt === 1) {
            return d.f + ':f64=' + d.f64 + '(0x' + d.hex + ')';
        } else if (d.str !== undefined) {
            return d.f + ':' + JSON.stringify(d.str);
        } else {
            return d.f + ':b64=' + d.b64;
        }
    }).join(' ');
}

// Lists fields in buf that the protobufjs type does not declare: [{ path, wt, field: describeField }].
// Known names are used for declared path segments, numbers for undeclared ones.
function undeclaredFields(buf, type, prefix, out) {
    out = out || [];
    prefix = prefix || '';
    let fields;
    try {
        fields = parseFields(buf);
    } catch(error) {
        out.push({ path: prefix + '(unparseable)', error: error.message });
        return out;
    }
    fields.forEach(field => {
        const declared = type && type.fieldsById && type.fieldsById[field.f];
        if (!declared) {
            out.push({ path: prefix + field.f, wt: field.wt, field: describeField(field, 0) });
            return;
        }
        try {
            declared.resolve();
        } catch(error) {
            // Unresolvable declared type: treat as opaque
        }
        const sub = declared.resolvedType;
        if (field.wt === 2 && !declared.map && sub && sub.fieldsById) {
            undeclaredFields(field.buf, sub, prefix + declared.name + '.', out);
        }
    });
    return out;
}

// ---- Logger ----

class RawDebugLogger {
    // dir: Homebridge storage path (no dir = permanently off). log: function(message) for the few status lines.
    constructor(dir, log, options) {
        options = options || {};
        this.dir = dir || null;
        this.flagPath = dir ? path.join(dir, FLAG_FILE) : null;
        this.outPath = dir ? path.join(dir, OUTPUT_FILE) : null;
        this.log = log || function() {};
        this.maxBytes = options.maxBytes || DEFAULT_MAX_BYTES;
        this.checkIntervalMs = options.checkIntervalMs !== undefined ? options.checkIntervalMs : FLAG_CHECK_INTERVAL_MS;
        this.lastCheck = null;
        this.enabled = false;
        this.bytesWritten = null;
        this.capped = false;
        this.reportedError = false;
        this.resourceTypes = {};
        this.frameSeq = 0;
    }

    active() {
        if (!this.flagPath) {
            return false;
        }
        const now = Date.now();
        if (this.lastCheck === null || now - this.lastCheck >= this.checkIntervalMs || now < this.lastCheck) {
            this.lastCheck = now;
            const was = this.enabled;
            try {
                this.enabled = fs.existsSync(this.flagPath);
            } catch(error) {
                this.enabled = false;
            }
            if (this.enabled && !was) {
                this.bytesWritten = null;
                this.capped = false;
                this.reportedError = false;
                this.log('Nest raw debug logging ON (' + this.flagPath + ' exists), writing ' + this.outPath);
            } else if (!this.enabled && was) {
                this.log('Nest raw debug logging OFF');
            }
        }
        return this.enabled;
    }

    write(record) {
        if (this.capped) {
            return;
        }
        const line = JSON.stringify(record) + '\n';
        const size = Buffer.byteLength(line);
        if (this.bytesWritten === null) {
            try {
                this.bytesWritten = fs.statSync(this.outPath).size;
            } catch(error) {
                this.bytesWritten = 0;
            }
        }
        if (this.bytesWritten + size > this.maxBytes) {
            this.capped = true;
            this.log('Nest raw debug logging: ' + this.outPath + ' reached ' + this.maxBytes + ' bytes, no longer writing (delete or move it and re-create the flag file to resume)');
            return;
        }
        fs.appendFileSync(this.outPath, line, { mode: 0o600 });
        this.bytesWritten += size;
    }

    // frame: one complete observe stream message (a StreamBody). ctx: { stream, decodeError,
    // decodeTrait(typeUrl, buf) -> object, legacyType(resourceId) -> { device_type, resource_type } | null }
    handleFrame(frame, ctx) {
        try {
            if (!this.active() || this.capped) {
                return;
            }
            this.processFrame(frame, ctx || {});
        } catch(error) {
            if (!this.reportedError) {
                this.reportedError = true;
                this.log('Nest raw debug logging error (logged once, continuing): ' + (error && error.message));
            }
        }
    }

    processFrame(frame, ctx) {
        const t = new Date().toISOString();
        const seq = ++this.frameSeq;
        const base = { t: t, stream: ctx.stream, seq: seq };
        const record = Object.assign({ kind: 'frame' }, base, { bytes: frame.length });
        if (ctx.decodeError) {
            record.plugin_decode_error = true;
        }

        let top;
        try {
            top = parseFields(frame);
        } catch(error) {
            record.parse_error = error.message;
            this.write(record);
            return;
        }

        const messages = top.filter(f => f.f === 1 && f.wt === 2);
        record.noop = top.filter(f => f.f === 15).length;
        const status = top.filter(f => f.f === 2);
        if (status.length) {
            record.status = status.map(f => describeField(f, 0));
        }
        const other = top.filter(f => f.f !== 1 && f.f !== 2 && f.f !== 15);
        if (other.length) {
            record.extra = other.map(f => this.describeExtra(String(f.f), f));
        }

        const gets = [], sets = [];
        record.messages = messages.map(m => {
            const fields = parseFields(m.buf);
            const summary = { sets: 0, gets: 0 };
            fields.forEach(f => {
                if (f.f === 3 && f.wt === 2) {
                    summary.gets++;
                    gets.push(f.buf);
                } else if (f.f === 1 && f.wt === 2) {
                    summary.sets++;
                    sets.push(f.buf);
                } else {
                    summary.extra = summary.extra || [];
                    summary.extra.push(this.describeExtra('message.' + f.f, f));
                }
            });
            return summary;
        });
        this.write(record);

        const parsedGets = gets.map(buf => this.parseGet(buf));
        // Learn resource types from peer_devices first, so a sensor that is offline (and so never mounted by the
        // plugin) is still classified
        parsedGets.forEach(g => {
            if (g && g.key === 'peer_devices' && g.valueBuf) {
                this.learnPeerDevices(ctx, g);
            }
        });

        parsedGets.forEach(g => {
            if (!g) {
                return;
            }
            const resource = this.classify(g.id, ctx);
            if (!resource || !this.wanted(resource.device_type, g)) {
                return;
            }
            const line = Object.assign({ kind: 'trait' }, base, {
                resource: g.id,
                device_type: resource.device_type,
                resource_type: resource.resource_type,
                key: g.key,
                type_url: g.typeUrl
            });
            if (g.uuid) {
                line.uuid = g.uuid;
            }
            if (g.valueBuf) {
                line.value = this.decodeTrait(ctx, g.typeUrl, g.valueBuf);
                line.raw = g.valueBuf.toString('base64');
            }
            if (g.extra.length) {
                line.extra = g.extra;
            }
            line.envelope = g.buf.toString('base64');
            this.write(line);
        });

        sets.forEach(buf => {
            const id = this.setResourceId(buf);
            const resource = id && this.classify(id, ctx);
            if (resource && (resource.device_type === 'kryptonite' || resource.device_type === 'thermostat')) {
                let decoded;
                try {
                    decoded = decodeGeneric(buf);
                } catch(error) {
                    decoded = null;
                }
                this.write(Object.assign({ kind: 'set' }, base, { resource: id, device_type: resource.device_type, generic: decoded, raw: buf.toString('base64') }));
            }
        });
    }

    describeExtra(pathName, field) {
        if (field.wt === 2 && field.buf.length > MAX_EXTRA_FIELD_BYTES) {
            return { path: pathName, wt: 2, len: field.buf.length, truncated: true };
        }
        return { path: pathName, wt: field.wt, field: describeField(field, 0) };
    }

    // TraitGetProperty { ObjectIdPair object = 1 { id = 1, key = 2, uuid = 3 }; DynamicProp_Indirect data = 3
    // { Any property = 1 { type_url = 1, value = 2 } } }; everything else is collected in extra
    parseGet(buf) {
        let fields;
        try {
            fields = parseFields(buf);
        } catch(error) {
            return null;
        }
        const g = { buf: buf, id: null, key: null, uuid: null, typeUrl: null, valueBuf: null, extra: [] };
        const sub = (field, pathName, handler) => {
            let inner;
            try {
                inner = field.wt === 2 ? parseFields(field.buf) : null;
            } catch(error) {
                inner = null;
            }
            if (!inner) {
                g.extra.push(this.describeExtra(pathName, field));
                return;
            }
            inner.forEach(handler);
        };
        fields.forEach(f => {
            if (f.f === 1) {
                sub(f, 'object', o => {
                    if (o.wt === 2 && o.f === 1) {
                        g.id = o.buf.toString('utf8');
                    } else if (o.wt === 2 && o.f === 2) {
                        g.key = o.buf.toString('utf8');
                    } else if (o.wt === 2 && o.f === 3) {
                        g.uuid = o.buf.toString('utf8');
                    } else {
                        g.extra.push(this.describeExtra('object.' + o.f, o));
                    }
                });
            } else if (f.f === 3) {
                sub(f, 'data', d => {
                    if (d.f === 1) {
                        sub(d, 'data.property', a => {
                            if (a.wt === 2 && a.f === 1) {
                                g.typeUrl = a.buf.toString('utf8');
                            } else if (a.wt === 2 && a.f === 2) {
                                g.valueBuf = a.buf;
                            } else {
                                g.extra.push(this.describeExtra('data.property.' + a.f, a));
                            }
                        });
                    } else {
                        g.extra.push(this.describeExtra('data.' + d.f, d));
                    }
                });
            } else {
                g.extra.push(this.describeExtra(String(f.f), f));
            }
        });
        return g.id ? g : null;
    }

    // The first field of a "set" element is either the resource id or an ObjectIdPair holding it
    setResourceId(buf) {
        try {
            const first = parseFields(buf).filter(f => f.f === 1 && f.wt === 2)[0];
            if (!first) {
                return null;
            }
            if (isPrintable(first.buf)) {
                return first.buf.toString('utf8');
            }
            const id = parseFields(first.buf).filter(f => f.f === 1 && f.wt === 2)[0];
            return id && isPrintable(id.buf) ? id.buf.toString('utf8') : null;
        } catch(error) {
            return null;
        }
    }

    decodeTrait(ctx, typeUrl, buf) {
        if (!ctx.decodeTrait || !typeUrl) {
            return null;
        }
        try {
            return ctx.decodeTrait(typeUrl, buf);
        } catch(error) {
            return { decode_error: error.message };
        }
    }

    learnPeerDevices(ctx, g) {
        const value = this.decodeTrait(ctx, g.typeUrl, g.valueBuf);
        ((value && value.devices) || []).forEach(el => {
            try {
                const id = el.data.deviceId.value, type = el.data.deviceType.value;
                if (id && type) {
                    this.resourceTypes[id] = { device_type: deviceTypeFor(type), resource_type: type };
                }
            } catch(error) {
                // Skip malformed entries
            }
        });
    }

    classify(id, ctx) {
        if (this.resourceTypes[id]) {
            return this.resourceTypes[id];
        }
        let legacy = null;
        try {
            legacy = ctx.legacyType ? ctx.legacyType(id) : null;
        } catch(error) {
            legacy = null;
        }
        if (legacy) {
            return {
                device_type: legacy.device_type === 'device' ? 'thermostat' : legacy.device_type,
                resource_type: legacy.resource_type
            };
        }
        return null;
    }

    wanted(deviceType, g) {
        if (deviceType === 'kryptonite') {
            return true;
        }
        if (deviceType === 'thermostat') {
            // The trait label, or the type's own name without its package (nest.trait.hvac.* would match everything)
            return THERMOSTAT_TRAIT_PATTERN.test(g.key || '') || THERMOSTAT_TRAIT_PATTERN.test((g.typeUrl || '').split('.').pop());
        }
        return false;
    }
}

function deviceTypeFor(resourceType) {
    if (/Kryptonite/.test(resourceType)) {
        return 'kryptonite';
    }
    if (THERMOSTAT_RESOURCE_TYPES.includes(resourceType)) {
        return 'thermostat';
    }
    return 'other';
}

module.exports = {
    RawDebugLogger: RawDebugLogger,
    parseFields: parseFields,
    describeField: describeField,
    decodeGeneric: decodeGeneric,
    formatGeneric: formatGeneric,
    undeclaredFields: undeclaredFields,
    FLAG_FILE: FLAG_FILE,
    OUTPUT_FILE: OUTPUT_FILE
};
