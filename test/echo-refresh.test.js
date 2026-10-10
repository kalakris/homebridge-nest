'use strict';

// A pushed change Nest has not echoed after API_MERGE_ECHO_MAX_SECONDS is decided by a fresh read (a restarted
// observe stream's first message), not by the cached pre-write state. 2026-10-06: a write Nest had applied was echoed
// only by a new stream ~10 min later; re-syncing from cache at 300 s looked like a reverted write.

const assert = require('assert');
// index.js installs Promise.delay, which the observe loop uses
require('../index.js');
const Connection = require('../lib/nest-connection');

const silent = { info() {}, debug() {}, error() {}, warn() {} };
const SETPOINTS = 'type.nestlabs.com/nest.trait.hvac.TargetTemperatureSettingsTrait';

async function withClock(fn) {
    const realNow = Date.now;
    let now = realNow();
    Date.now = () => now;
    const clock = { advance(ms) { now += ms; }, get now() { return now; } };
    try {
        return await fn(clock);
    } finally {
        Date.now = realNow;
    }
}

const tick = () => new Promise(resolve => setImmediate(resolve));

function makeConnection() {
    const conn = new Connection({}, silent, false, false);
    conn.pushUpdatesDebounced = () => {};
    conn.pushUpdates = () => {}; // mode changes push at once
    conn.connected = true;
    conn.token = 'token';
    conn.currentState = {
        structure: { S1: { structure_id: 'S1', swarm: ['device.T1', 'device.T2'] } },
        where: { S1: { wheres: [] } },
        device: { T1: { target_temperature_type: 'range', can_heat: true, can_cool: true }, T2: { target_temperature_type: 'range', can_heat: true, can_cool: true } },
        shared: {
            T1: { target_temperature_type: 'range', target_temperature_low: 20, target_temperature_high: 22 },
            T2: { target_temperature_type: 'range', target_temperature_low: 18, target_temperature_high: 21 }
        }
    };
    conn.resyncs = [];
    conn.updateHomeKit = data => conn.resyncs.push(data.devices.thermostats.T1.target_temperature_low);
    conn.restarts = 0;
    conn.observeStreamSeq = 1;
    conn.observeRestart = () => conn.restarts++;
    return conn;
}

const forced = (conn, id) => conn.apiResponseToObjectTree(conn.mergePendingUpdates(conn.currentState)).devices.thermostats[id || 'T1'].target_temperature_low;

// A write of T1's low setpoint to 21 (Nest had 20), pushed and then never echoed for API_MERGE_ECHO_MAX_SECONDS
async function unechoedWrite(clock, conn) {
    conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
    conn.settleMergeUpdates(clock.now, true);
    clock.advance(301000);
    conn.scheduleMergeEnd();
    await tick();
}

// What the observe stream does with a new stream's first message: merge, release, then notify HomeKit with the result
function snapshot(conn, traits) {
    conn.observeStreamSeq++;
    conn.releaseEchoedMergeUpdates(conn.currentState, conn.refreshSnapshotTraits({ hasDeviceInfo: true, traits: traits }));
}

const SNAPSHOT_T1 = [['target_temperature_settings', 'DEVICE_T1', SETPOINTS], ['hvac_control', 'DEVICE_T1', 'x']];

function done(conn) {
    clearTimeout(conn.mergeEndTimer);
}

module.exports = async function() {
    // Reaching the echo limit restarts the observe stream instead of re-syncing HomeKit from the cached pre-write value
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        assert.deepStrictEqual(conn.resyncs, [], 'no re-sync to the cached pre-write value');
        assert.strictEqual(forced(conn), 21, 'still forced while the refresh is under way');
        assert.strictEqual(conn.restarts, 1, 'one stream restart');
        done(conn);
    });

    // The snapshot shows our value (the 2026-10-06 case): released, and HomeKit keeps it
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        conn.currentState.shared.T1.target_temperature_low = 21;
        snapshot(conn, SNAPSHOT_T1);
        assert.strictEqual(conn.mergeUpdates.length, 0, 'released');
        assert.strictEqual(forced(conn), 21);
        assert.deepStrictEqual(conn.resyncs, []);
        assert.strictEqual(conn.echoRefresh, null);
        assert.strictEqual(conn.mergeEndTimer, null);
    });

    // The snapshot still shows the pre-write value (the write did not land): released, HomeKit gets Nest's value
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        snapshot(conn, SNAPSHOT_T1);
        assert.strictEqual(conn.mergeUpdates.length, 0);
        assert.strictEqual(forced(conn), 20);
    });

    // The snapshot shows someone else's value: that is the truth
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        conn.currentState.shared.T1.target_temperature_low = 19.5;
        snapshot(conn, SNAPSHOT_T1);
        assert.strictEqual(conn.mergeUpdates.length, 0);
        assert.strictEqual(forced(conn), 19.5);
    });

    // Only a message on a NEW stream counts, and only for a setting whose trait it carried: anything else is the
    // previous stream's cached value. The refresh stays open until each of its changes is decided, so a snapshot split
    // over several messages still decides them all.
    await withClock(async clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.commitUpdate('shared.T2', { target_temperature_low: 19 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(301000);
        conn.scheduleMergeEnd();
        await tick();
        assert.strictEqual(conn.restarts, 1);
        conn.releaseEchoedMergeUpdates(conn.currentState, conn.refreshSnapshotTraits({ hasDeviceInfo: true, traits: SNAPSHOT_T1 }));
        assert.strictEqual(forced(conn), 21, 'message on the old stream');
        conn.currentState.shared.T2.target_temperature_low = 19;
        snapshot(conn, [['target_temperature_settings', 'DEVICE_T2', SETPOINTS], ['hvac_control', 'DEVICE_T1', 'x']]);
        assert.strictEqual(forced(conn), 21, 'T1 not decided by a message without its setpoints');
        assert.strictEqual(forced(conn, 'T2'), 19);
        assert.deepStrictEqual(conn.mergeUpdates.map(u => u.object.object_key), ['shared.T1'], 'T2 decided');
        assert.notStrictEqual(conn.echoRefresh, null, 'the refresh is still open for T1');
        conn.releaseEchoedMergeUpdates(conn.currentState, conn.refreshSnapshotTraits({ hasDeviceInfo: false, traits: SNAPSHOT_T1 }));
        assert.strictEqual(conn.mergeUpdates.length, 0, 'T1 decided by a later message on the new stream');
        assert.strictEqual(forced(conn), 20);
        assert.strictEqual(conn.echoRefresh, null, 'refresh closed once all its changes are decided');
    });

    // What counts as a snapshot carrying a change's setting (ids as translateProperty maps them)
    {
        const conn = makeConnection();
        conn.legacyStructureMap = { abc: 'S1' };
        assert.ok(conn.snapshotCovers(SNAPSHOT_T1, 'shared.T1'));
        assert.ok(!conn.snapshotCovers([['hvac_control', 'DEVICE_T1', 'x']], 'shared.T1'), 'setpoints need their own trait');
        assert.ok(conn.snapshotCovers([['hvac_control', 'DEVICE_T1', 'x']], 'device.T1'));
        assert.ok(!conn.snapshotCovers(SNAPSHOT_T1, 'device.T2'));
        assert.ok(conn.snapshotCovers([['structure_info', 'STRUCTURE_abc', 'x']], 'structure.S1'));
        assert.ok(!conn.snapshotCovers([['structure_info', 'STRUCTURE_def', 'x']], 'structure.S1'));
    }

    // No snapshot within the refresh timeout (the refresh failed): today's behaviour, one re-sync from cache, no retry
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        clock.advance(29000);
        conn.scheduleMergeEnd();
        assert.strictEqual(forced(conn), 21);
        clock.advance(2000);
        conn.scheduleMergeEnd();
        await tick();
        assert.deepStrictEqual(conn.resyncs, [20], 're-synced once on refresh timeout');
        assert.strictEqual(forced(conn), 20);
        assert.strictEqual(conn.restarts, 1, 'no second restart');
        assert.strictEqual(conn.mergeEndTimer, null);
        assert.strictEqual(conn.echoRefresh, null, 'refresh closed on timeout');
    });

    // Disconnected at the echo limit: no stream to restart, today's behaviour at once
    await withClock(async clock => {
        const conn = makeConnection();
        conn.connected = false;
        await unechoedWrite(clock, conn);
        assert.deepStrictEqual(conn.resyncs, [20]);
        assert.strictEqual(conn.restarts, 0);
    });

    // Several changes expiring close together share one restart; one due later gets its own, an interval on
    await withClock(async clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.commitUpdate('shared.T2', { target_temperature_low: 19 }, 'range', true);
        conn.commitUpdate('shared.T1', { target_temperature_high: 23 }, 'range', true);
        // pushed (one push each) at +0, +20 s and +90 s
        [0, 20000, 90000].forEach((pushed, i) => Object.assign(conn.mergeUpdates[i], { pushed: true, expiry_time: clock.now + pushed + 300000 }));
        clock.advance(301000); // the first write is due; the second is 19 s off, the third 89 s
        conn.scheduleMergeEnd();
        await tick();
        assert.strictEqual(conn.restarts, 1);
        assert.deepStrictEqual(conn.mergeUpdates.map(u => !!u.refreshing), [true, true, false]);
        conn.currentState.shared.T1.target_temperature_low = 21;
        conn.currentState.shared.T2.target_temperature_low = 19;
        snapshot(conn, SNAPSHOT_T1.concat([['target_temperature_settings', 'DEVICE_T2', SETPOINTS]]));
        assert.strictEqual(conn.mergeUpdates.length, 1, 'both refreshed writes decided by one snapshot');
        assert.strictEqual(forced(conn, 'T2'), 19);
        clock.advance(90000);
        conn.scheduleMergeEnd();
        await tick();
        assert.strictEqual(conn.restarts, 2, 'the third write gets its own refresh');
        assert.deepStrictEqual(conn.resyncs, []);
        done(conn);
    });

    // Never more than one restart per interval: a change due sooner waits, still forced
    await withClock(async clock => {
        const conn = makeConnection();
        conn.lastEchoRefreshTime = clock.now + 301000 - 10000; // a refresh started 10 s before this write is due
        await unechoedWrite(clock, conn);
        assert.strictEqual(conn.restarts, 0);
        assert.strictEqual(forced(conn), 21);
        clock.advance(50000);
        conn.scheduleMergeEnd();
        await tick();
        assert.strictEqual(conn.restarts, 1);
        assert.deepStrictEqual(conn.resyncs, []);
        done(conn);
    });

    // The restart is skipped if the stream has been replaced meanwhile (its successor's first message serves)
    await withClock(async clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(301000);
        conn.scheduleMergeEnd();
        conn.observeStreamSeq++;
        await tick();
        assert.strictEqual(conn.restarts, 0);
        conn.releaseEchoedMergeUpdates(conn.currentState, conn.refreshSnapshotTraits({ hasDeviceInfo: true, traits: SNAPSHOT_T1 }));
        assert.strictEqual(conn.mergeUpdates.length, 0);
    });

    // A HomeKit mode change through update(): it writes device.eco = { mode: 'schedule' } (an object, compared by
    // content) and shared { target_change_pending, target_temperature_type } (target_change_pending is never in
    // Nest's state, so the mode alone is the echo). Both hold until echoed, and need no refresh.
    await withClock(async clock => {
        const conn = makeConnection();
        conn.currentState.device.T1.eco = { mode: 'manual-eco' };
        conn.update('shared.T1', 'hvac_mode', 'heat', 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        assert.strictEqual(conn.mergeUpdates.length, 2);
        const forcedEco = () => conn.mergePendingUpdates(conn.currentState).device.T1.eco.mode;
        conn.currentState.device.T1.eco = { mode: 'manual-eco' }; // a frame: Nest has not applied it yet
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 2, 'not taken for another change');
        assert.strictEqual(forcedEco(), 'schedule');
        conn.currentState.device.T1.eco = { mode: 'schedule' };
        conn.currentState.shared.T1.target_temperature_type = 'heat';
        conn.releaseEchoedMergeUpdates(conn.currentState);
        assert.strictEqual(conn.mergeUpdates.length, 0, 'released by the echo');
        clock.advance(301000);
        conn.scheduleMergeEnd();
        await tick();
        assert.strictEqual(conn.restarts, 0, 'no refresh');
        assert.deepStrictEqual(conn.resyncs, []);
    });

    // A later push of another setting (here another thermostat's) does not touch an earlier pushed change: a success
    // does not pull it out of its refresh (or restart its echo wait), a failure does not drop it. A later push of the
    // SAME setting supersedes it on success: supersede.test.js.
    await withClock(async clock => {
        const conn = makeConnection();
        await unechoedWrite(clock, conn);
        const expiry = conn.mergeUpdates[0].expiry_time;
        conn.commitUpdate('shared.T2', { target_temperature_low: 19 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        assert.strictEqual(conn.mergeUpdates[0].refreshing, true);
        assert.strictEqual(conn.mergeUpdates[0].expiry_time, expiry);
        snapshot(conn, SNAPSHOT_T1);
        assert.strictEqual(forced(conn), 20, 'decided by its refresh');
        done(conn);
    });
    await withClock(async clock => {
        const conn = makeConnection();
        conn.commitUpdate('shared.T1', { target_temperature_low: 21 }, 'range', true);
        conn.settleMergeUpdates(clock.now, true);
        clock.advance(60000);
        conn.commitUpdate('shared.T2', { target_temperature_low: 19 }, 'range', true);
        conn.settleMergeUpdates(clock.now, false);
        assert.strictEqual(forced(conn), 21, 'the pushed write still holds');
        assert.strictEqual(forced(conn, 'T2'), 18, 'the failed one is dropped');
        assert.deepStrictEqual(conn.resyncs, [21], 'HomeKit re-synced with T1 still forced');
        done(conn);
    });

    // A renewed stream's first message (Nest's full state) reaches HomeKit; upstream handed it to the spent resolver
    {
        const conn = new Connection({}, silent, false, false);
        conn.connected = true;
        conn.token = 'token';
        const resolved = [], handled = [];
        let streams = 0;
        conn.updateProtobufData = resolve => {
            streams++;
            if (streams <= 2) {
                resolve({ stream: streams });
                return Promise.resolve();
            }
            return new Promise(() => {});
        };
        conn.protobufDataTimerLoop(data => resolved.push(data), data => handled.push(data));
        for (let i = 0; i < 100 && streams < 3; i++) {
            await new Promise(resolve => setTimeout(resolve, 10));
        }
        assert.deepStrictEqual(resolved, [{ stream: 1 }], 'the loop\'s own resolve(null) after a stream is ignored');
        assert.deepStrictEqual(handled, [{ stream: 2 }]);
    }
};
