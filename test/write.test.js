'use strict';

const assert = require('assert');
const axios = require('axios');
const Connection = require('../lib/nest-connection');

const errors = [];
const logger = { info() {}, debug() {}, warn() {}, error(...args) { errors.push(args.join(' ')); } };

function makeConnection() {
    const conn = new Connection({}, logger, false, false);
    conn.connected = true;
    conn.token = 'token';
    conn.transport_url = 'https://transport.invalid';
    conn.pushUpdatesDebounced = () => {};
    conn.updateHomeKit = () => {};
    conn.auth = () => Promise.resolve(true);
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.T1'] } },
        where: { S1: { wheres: [] } },
        device: { T1: { target_temperature_type: 'range', can_heat: true, can_cool: true } },
        shared: { T1: { target_temperature_type: 'range', target_temperature_low: 20, target_temperature_high: 22 } }
    };
    conn.accessories = { T1: { device: { target_temperature_type: 'range', target_temperature_low: 21, target_temperature_high: 22 } } };
    return conn;
}

async function waitForProto(conn) {
    for (let i = 0; i < 200 && !conn.TraitMap; i++) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.ok(conn.TraitMap, 'protobuf definitions loaded');
}

module.exports = async function() {
    const realAdapter = axios.defaults.adapter;
    try {
        // A protobuf push that fails with 401 is retried after reauthentication (upstream dropped it)
        {
            const conn = makeConnection();
            await waitForProto(conn);
            const requests = [];
            axios.defaults.adapter = async config => {
                requests.push(config.url);
                if (requests.length == 1) {
                    const error = new Error('Request failed with status code 401');
                    error.response = { status: 401, headers: {}, data: '' };
                    error.config = config;
                    throw error;
                }
                return { data: Buffer.alloc(0), status: 200, statusText: 'OK', headers: {}, config: config };
            };
            conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
            await conn.pushUpdates();
            assert.strictEqual(requests.length, 2, 'retried: ' + requests.join(', '));
            assert.ok(requests.every(url => url.endsWith('/nestlabs.gateway.v1.TraitBatchApi/BatchUpdateState')));
            assert.strictEqual(conn.pendingUpdates.length, 0);
            assert.strictEqual(conn.failedPushAPICalls, 0);
            clearTimeout(conn.mergeEndTimer);
        }

        // A non-retryable failure is logged with what was lost, and the forced value is dropped
        {
            errors.length = 0;
            const conn = makeConnection();
            await waitForProto(conn);
            const resyncs = [];
            conn.updateHomeKit = data => resyncs.push(data.devices.thermostats.T1.target_temperature_low);
            axios.defaults.adapter = async config => {
                const error = new Error('Request failed with status code 400');
                error.response = { status: 400, headers: {}, data: '' };
                error.config = config;
                throw error;
            };
            conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
            await conn.pushUpdates();
            assert.ok(errors.some(line => /NOT applied: shared\.T1 \{"target_temperature_low":21\}/.test(line)), errors.join('\n'));
            assert.deepStrictEqual(resyncs, [20]);
            assert.ok(!errors.some(line => /token|cookie/i.test(line)), 'no credentials in the log');
        }

        // Not connected: dropped loudly rather than silently
        {
            errors.length = 0;
            const conn = makeConnection();
            conn.connected = false;
            conn.commitUpdate('shared.T1', { target_temperature_high: 23 }, 'range', true);
            await conn.pushUpdates();
            assert.ok(errors.some(line => /Not connected to Nest - dropping changes/.test(line)), errors.join('\n'));
        }
    } finally {
        axios.defaults.adapter = realAdapter;
    }
};
