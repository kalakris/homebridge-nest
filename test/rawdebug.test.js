'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const protobuf = require('protobufjs');
const { RawDebugLogger, decodeGeneric, formatGeneric, undeclaredFields } = require('../lib/raw-debug.js');

function varint(n) {
    const out = [];
    while (n > 127) {
        out.push((n & 0x7f) | 0x80);
        n = Math.floor(n / 128);
    }
    out.push(n);
    return Buffer.from(out);
}

function ld(field, buf) {
    buf = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
    return Buffer.concat([varint(field * 8 + 2), varint(buf.length), buf]);
}

function vfield(field, n) {
    return Buffer.concat([varint(field * 8), varint(n)]);
}

module.exports = async function() {
    const root = await protobuf.load(path.join(__dirname, '..', 'lib', 'protobuf', 'root.proto'));
    root.resolveAll();
    const Temperature = root.lookupType('nest.trait.sensor.TemperatureTrait');

    // A temperature trait with an undeclared field 99 (varint 5) and an undeclared fixed32 inside temperature.value
    const declared = Buffer.from(Temperature.encode(Temperature.fromObject({ temperature: { value: { value: 20.5 } } })).finish());
    const inner = ld(1, Buffer.concat([ld(1, Buffer.from([0x0d, 0x00, 0x00, 0xa4, 0x41])), Buffer.from([0x15, 0x00, 0x00, 0x80, 0x3f])]));
    assert.strictEqual(declared.toString('hex'), ld(1, ld(1, Buffer.from([0x0d, 0x00, 0x00, 0xa4, 0x41]))).toString('hex'));
    const raw = Buffer.concat([inner, vfield(99, 5)]);

    const generic = decodeGeneric(raw);
    assert.strictEqual(generic.length, 2);
    assert.strictEqual(generic[0].sub[0].sub[0].f32, 20.5);
    assert.strictEqual(generic[0].sub[1].f32, 1);
    assert.deepStrictEqual({ f: generic[1].f, wt: generic[1].wt, v: generic[1].v }, { f: 99, wt: 0, v: 5 });
    assert.strictEqual(formatGeneric(generic), '1{1{1:f32=20.5(u32=1101266944)} 2:f32=1(u32=1065353216)} 99:v=5');
    assert.deepStrictEqual(undeclaredFields(raw, Temperature).map(e => e.path), ['temperature.2', '99']);

    // Varints above 2^53 keep their exact value as hex
    const big = decodeGeneric(Buffer.from([0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]));
    assert.strictEqual(big[0].hex, 'ffffffffffffffff');
    // Printable text stays text
    assert.strictEqual(decodeGeneric(ld(1, 'DEVICE_1234'))[0].str, 'DEVICE_1234');

    // A frame: StreamBody { message { get { object { id, key }, 2: undeclared, data { property { type_url, value } } } } }
    const get = Buffer.concat([
        ld(1, Buffer.concat([ld(1, 'DEVICE_K1'), ld(2, 'current_temperature')])),
        ld(2, Buffer.from([0x01, 0x02])),
        ld(3, ld(1, Buffer.concat([ld(1, 'type.nestlabs.com/nest.trait.sensor.TemperatureTrait'), ld(2, raw)])))
    ]);
    const ignored = Buffer.concat([
        ld(1, Buffer.concat([ld(1, 'STRUCTURE_S1'), ld(2, 'structure_info')])),
        ld(3, ld(1, ld(1, 'type.nestlabs.com/nest.trait.structure.StructureInfoTrait')))
    ]);
    const frame = ld(1, Buffer.concat([ld(3, get), ld(3, ignored), vfield(5, 1700000000)]));

    const ctx = {
        stream: 3,
        decodeTrait: (typeUrl, buf) => Temperature.toObject(Temperature.decode(buf)),
        legacyType: id => id === 'DEVICE_K1' ? { device_type: 'kryptonite', resource_type: 'nest.resource.NestKryptoniteResource' } : null
    };

    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-raw-debug-'));
    const out = path.join(dir, 'nest-raw-debug.jsonl');
    const messages = [];
    try {
        // Off without a storage path, and without the flag file
        assert.strictEqual(new RawDebugLogger(undefined, () => {}).active(), false);
        let logger = new RawDebugLogger(dir, m => messages.push(m));
        logger.handleFrame(frame, ctx);
        assert.ok(!fs.existsSync(out));

        // The flag file is re-checked only after the interval
        fs.writeFileSync(path.join(dir, 'nest-raw-debug.on'), '');
        logger.handleFrame(frame, ctx);
        assert.ok(!fs.existsSync(out));
        logger.lastCheck -= 31 * 1000;
        logger.handleFrame(frame, ctx);
        const lines = fs.readFileSync(out, 'utf8').trim().split('\n').map(l => JSON.parse(l));
        assert.strictEqual(lines.length, 2);
        assert.strictEqual(lines[0].kind, 'frame');
        assert.strictEqual(lines[0].stream, 3);
        assert.strictEqual(lines[0].messages[0].gets, 2);
        assert.deepStrictEqual(lines[0].messages[0].extra.map(e => [e.path, e.field.v]), [['message.5', 1700000000]]);
        assert.strictEqual(lines[1].kind, 'trait');
        assert.strictEqual(lines[1].resource, 'DEVICE_K1');
        assert.strictEqual(lines[1].key, 'current_temperature');
        assert.strictEqual(lines[1].value.temperature.value.value, 20.5);
        assert.strictEqual(lines[1].raw, raw.toString('base64'));
        assert.deepStrictEqual(lines[1].extra.map(e => e.path), ['2']);
        assert.strictEqual(fs.statSync(out).mode & 0o777, 0o600);

        // Resource types are learnt from peer_devices (offline sensors are never mounted by the plugin), and a
        // thermostat logs only its temperature/humidity/HVAC traits
        const PeerDevices = root.lookupType('weave.trait.peerdevices.PeerDevicesTrait');
        const peers = Buffer.from(PeerDevices.encode(PeerDevices.fromObject({ devices: [
            { data: { deviceId: { value: 'DEVICE_K2' }, deviceType: { value: 'nest.resource.NestKryptoniteResource' } } },
            { data: { deviceId: { value: 'DEVICE_T2' }, deviceType: { value: 'nest.resource.NestLearningThermostat3Resource' } } }
        ] })).finish());
        const trait = (id, key, typeName, value) => ld(3, Buffer.concat([
            ld(1, Buffer.concat([ld(1, id), ld(2, key)])),
            ld(3, ld(1, Buffer.concat([ld(1, 'type.nestlabs.com/' + typeName), ld(2, value)])))
        ]));
        const peerFrame = ld(1, Buffer.concat([
            trait('STRUCTURE_S1', 'peer_devices', 'weave.trait.peerdevices.PeerDevicesTrait', peers),
            trait('DEVICE_K2', 'liveness', 'weave.trait.heartbeat.LivenessTrait', Buffer.alloc(0)),
            trait('DEVICE_T2', 'current_temperature', 'nest.trait.sensor.TemperatureTrait', raw),
            trait('DEVICE_T2', 'display_settings', 'nest.trait.hvac.DisplaySettingsTrait', Buffer.alloc(0))
        ]));
        fs.unlinkSync(out);
        logger.handleFrame(peerFrame, {
            stream: 4,
            decodeTrait: (typeUrl, buf) => {
                const type = root.lookupType(typeUrl.split('/')[1]);
                return type.toObject(type.decode(buf), { enums: String, defaults: true, bytes: String });
            }
        });
        const peerLines = fs.readFileSync(out, 'utf8').trim().split('\n').map(l => JSON.parse(l)).filter(l => l.kind === 'trait');
        assert.deepStrictEqual(peerLines.map(l => [l.resource, l.device_type, l.key]), [['DEVICE_K2', 'kryptonite', 'liveness'], ['DEVICE_T2', 'thermostat', 'current_temperature']]);

        // Size cap: stops writing and says so once
        logger = new RawDebugLogger(dir, m => messages.push(m), { maxBytes: fs.statSync(out).size + 10 });
        const before = messages.length, sizeBefore = fs.statSync(out).size;
        logger.handleFrame(frame, ctx);
        logger.handleFrame(frame, ctx);
        assert.strictEqual(fs.statSync(out).size, sizeBefore);
        assert.strictEqual(messages.length - before, 2); // ON, then the cap warning
        assert.ok(/no longer writing/.test(messages[messages.length - 1]));

        // Garbage never throws
        logger = new RawDebugLogger(dir, () => {});
        fs.unlinkSync(out);
        logger.handleFrame(Buffer.from([0xff, 0xff, 0xff]), {});
        logger.handleFrame(null, {});
        assert.strictEqual(JSON.parse(fs.readFileSync(out, 'utf8').trim().split('\n')[0]).parse_error !== undefined, true);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
};
