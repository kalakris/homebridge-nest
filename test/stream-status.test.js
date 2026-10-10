'use strict';

// A stream's status (7: authentication failed, 13: internal error) belongs to that stream. The first message of every
// real stream is the resources frame, which the plugin's schema cannot decode and which therefore leaves
// lastProtobufCode as it is: with the status still set from the stream before, the next stream was closed at that
// first message, and the loop reauthenticated (7) or waited (13) again, every few seconds, until a stream happened to
// open with a decodable message (2026-10-10 13:49:56Z: status 7, reauthentication; 13:49:59Z: the next stream closed
// after one message, reauthentication again; 13:50:16Z: a bare status 4 ended it).
// Drives the real observe loop and decoding; only http2.connect and authentication are replaced.

const assert = require('assert');
const EventEmitter = require('events');
const http2 = require('http2');
const varint = require('varint');
require('../index.js');
const Connection = require('../lib/nest-connection');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const field = (number, payload) => Buffer.concat([Buffer.from(varint.encode((number << 3) | 2)), Buffer.from(varint.encode(payload.length)), Buffer.from(payload)]);
// The resources frame: resources and their trait types, no values; not a StreamBody the schema decodes
const resourcesFrame = field(1, Buffer.concat(['DEVICE_A', 'DEVICE_B'].map(id => field(1, Buffer.concat([field(1, id), field(2, 'nest.resource.NestOnyxResource'),
    field(4, Buffer.concat([field(1, 'target_temperature_settings'), field(2, 'nest.trait.hvac.TargetTemperatureSettingsTrait')]))])))));

module.exports = async function() {
    const realConnect = http2.connect, realDelay = Promise.delay;
    Promise.delay = ms => realDelay(Math.min(ms, 5));
    try {
        for (const code of [7, 13]) {
            const lines = [];
            const log = { info: (...a) => lines.push('info ' + a.join(' ')), error: (...a) => lines.push('error ' + a.map(String).join(' ')), debug() {}, warn() {} };
            const conn = new Connection({}, log, false, false);
            conn.connected = true;
            conn.token = 'token';
            let auths = 0;
            conn.authAttempt = async () => {
                auths++;
                return true;
            };
            for (let i = 0; i < 200 && !conn.StreamBody; i++) {
                await sleep(10);
            }
            assert.ok(conn.protobufToNestLegacy(resourcesFrame).decodeError, 'the resources frame does not decode');
            conn.lastProtobufCode = null;
            const statusFrame = Buffer.from(conn.StreamBody.encode(conn.StreamBody.fromObject({ status: { code: code, message: 'x' } })).finish());
            let streams = 0, closedAfterFirst = 0;
            http2.connect = () => {
                const client = new EventEmitter(), request = new EventEmitter();
                let destroyed = false;
                client.destroy = () => destroyed = true;
                client.ping = () => {};
                client.request = () => request;
                request.write = () => {};
                request.destroy = () => {};
                request.end = () => {
                    const n = ++streams;
                    setImmediate(() => {
                        if (n == 1) {
                            request.emit('data', statusFrame);
                        } else if (n <= 8) {
                            // The next streams open with the resources frame, as real ones do, and then stay open
                            request.emit('data', resourcesFrame);
                            if (destroyed) {
                                closedAfterFirst++;
                            }
                        }
                    });
                };
                return client;
            };
            conn.observe(() => {});
            await sleep(300);
            conn.ensureConnected = () => new Promise(() => {});
            assert.strictEqual(closedAfterFirst, 0, 'status ' + code + ': the next stream is not closed at its first message');
            assert.strictEqual(streams, 2, 'status ' + code + ': one stream after the one that ended');
            assert.strictEqual(auths, code == 7 ? 1 : 0, 'status ' + code + ': reauthenticated once for a 7, never for a 13');
            assert.strictEqual(lines.filter(line => /internal error/.test(line)).length, code == 13 ? 1 : 0);
            assert.strictEqual(conn.lastProtobufCode, null);
        }
    } finally {
        http2.connect = realConnect;
        Promise.delay = realDelay;
    }
};
