'use strict';

const assert = require('assert');
const Connection = require('../lib/nest-connection');

const silent = { info() {}, debug() {}, error() {}, warn() {} };

module.exports = async function() {
    const realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    try {
        const conn = new Connection({}, silent, false, false);
        let attempts = 0, succeed = false;
        conn.authAttempt = async () => {
            attempts++;
            await Promise.resolve();
            if (succeed) {
                conn.connected = true;
                conn.token = 'token';
            }
            return succeed;
        };

        // Disconnected (e.g. a rejected reauthentication): the loops keep retrying, with backoff
        assert.strictEqual(await conn.ensureConnected(), false);
        assert.strictEqual(attempts, 1);
        assert.strictEqual(await conn.ensureConnected(), false);
        assert.strictEqual(attempts, 1, 'no retry inside the backoff');
        now += 31000;
        assert.strictEqual(await conn.ensureConnected(), false);
        assert.strictEqual(attempts, 2, 'retried after 30 s');
        assert.strictEqual(conn.reconnectDelaySeconds, 120, 'backoff doubles');

        // Both data loops calling at once share one attempt
        now += 61000;
        const results = await Promise.all([conn.ensureConnected(), conn.ensureConnected()]);
        assert.deepStrictEqual(results, [false, false]);
        assert.strictEqual(attempts, 3);

        // Backoff is capped
        for (let i = 0; i < 10; i++) {
            now += conn.reconnectDelaySeconds * 1000 + 1000;
            await conn.ensureConnected();
        }
        assert.strictEqual(conn.reconnectDelaySeconds, 15 * 60);

        // Success resets the backoff
        succeed = true;
        now += 16 * 60 * 1000;
        assert.strictEqual(await conn.ensureConnected(), true);
        assert.strictEqual(conn.reconnectDelaySeconds, 30);
        assert.strictEqual(conn.disconnectedSince, null);
        const before = attempts;
        assert.strictEqual(await conn.ensureConnected(), true);
        assert.strictEqual(attempts, before, 'no attempt while connected');
    } finally {
        Date.now = realNow;
    }
};
