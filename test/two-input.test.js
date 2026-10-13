'use strict';

// A Nest Temperature Sensor's reading comes in twice: with the REST API's long poll and on the observe stream. The
// stream keeps everything it has ever delivered and merges all of it over the current state after every message, so
// when the REST API had a new reading first, the next message about anything put the stream's older reading back.
// 2026-10-13 (UTC, the sensor at 21.3): REST delivered 20.8 at 00:13:00, an unrelated message at 00:13:34.7 put 21.3
// back, the stream delivered 20.8 at 00:15:08.7. Home Assistant saw 20.8, 21.3, 20.8.
//
// Driven through the real REST and stream paths, decoding and accessories (two-input.js).

const assert = require('assert');
const path = require('path');
const twoInput = require('./two-input');

const near = (a, b) => typeof a == 'number' && typeof b == 'number' && Math.abs(a - b) < 1e-6;
const nearAll = (values, expected, text) => assert.ok(values.length == expected.length && values.every((el, i) => near(el, expected[i])), text + ': ' + JSON.stringify(values) + ', expected ' + JSON.stringify(expected));

module.exports = async function() {
    const houses = twoInput.create({
        Connection: require('../lib/nest-connection'),
        harness: require('./harness'),
        axios: require('axios'),
        protobuf: require('protobufjs'),
        protoPath: path.join(__dirname, '../lib/protobuf/root.proto')
    });
    await houses.prepare();
    try {
        // The sequence of 2026-10-13 00:13Z
        {
            const h = await houses.open();
            const t = h.traits;
            assert.ok(near(h.shown('K1'), 21.3) && near(await h.read('K1'), 21.3));
            await h.runTo(60);
            await h.rest('kryptonite.K1', { current_temperature: 20.8 });
            assert.ok(near(h.shown('K1'), 20.8), 'the REST API\'s reading is shown at once');
            await h.runTo(94.2);
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 20.8), 'an unrelated message does not put the older reading back: ' + h.shown('K1'));
            assert.ok(near(await h.read('K1'), 20.8));
            await h.runTo(95.2);
            await h.frame(t.temperature('T1', 20.79));
            assert.ok(near(h.shown('K1'), 20.8) && near(h.shown('T1'), 20.79), 'nor does the thermostat\'s own message');
            await h.runTo(120);
            await h.noop();
            assert.ok(near(h.shown('K1'), 20.8), 'nor an empty one');
            await h.runTo(188.2);
            await h.frame(t.temperature('K1', 20.8));
            await h.runTo(189);
            await h.frame(t.unrelated());
            nearAll(h.values('K1'), [20.8], 'HomeKit was sent one change');
            // The other sensors and the thermostats: nothing
            assert.deepStrictEqual(h.events.filter(el => el.id != 'K1').map(el => el.id + ' ' + el.name), ['T1 temperature']);
            assert.deepStrictEqual(h.problems, []);
            assert.deepStrictEqual(h.lines, [], 'nothing is logged');
            await h.close();
        }

        // 2026-10-09 17:07 to 17:16Z: the thermostat stops using the sensor for a few minutes (it shows its own
        // sensor's reading meanwhile) and takes it back at the sensor's next report. Its messages about that come
        // within seconds of the sensor's new reading, which the REST API has first: the older reading was back 1.75 s
        // after the new one, for 120 s, and again at the sensor's next report 5 minutes later.
        {
            const h = await houses.open({ state: {
                T1: { heat: 19, cool: 23, temperature: 21.85, backplate: 21.9, humidity: 50, hvac: 'off' },
                T2: { heat: 18.5, cool: 23.5, temperature: 20.4, backplate: 20.1, humidity: 52, hvac: 'off' },
                K1: { temperature: 20.8 }, K2: { temperature: 20.4 }, K3: { temperature: 19.6 }
            } });
            const t = h.traits;
            await h.runTo(100);
            await h.rest('kryptonite.K1', { current_temperature: 20.9 });
            await h.runTo(101.75);
            await h.frame(t.unrelated('T1'));
            await h.runTo(167.5);
            await h.frame(t.temperature('T1', 20.89));
            assert.ok(near(h.shown('K1'), 20.9) && near(h.shown('T1'), 20.89), 'the thermostat\'s messages leave the sensor\'s reading: ' + h.shown('K1'));
            await h.runTo(222);
            await h.frame(t.temperature('K1', 20.9));
            await h.runTo(403.6);
            await h.rest('kryptonite.K1', { current_temperature: 20.8 });
            await h.runTo(419.8);
            await h.frame(t.temperature('T1', 20.79));
            assert.ok(near(h.shown('K1'), 20.8) && near(h.shown('T1'), 20.79));
            await h.runTo(420.8);
            await h.frame(t.temperature('K1', 20.8));
            nearAll(h.values('K1'), [20.9, 20.8], 'the sensor');
            nearAll(h.values('T1'), [20.89, 20.79], 'the thermostat');
            assert.deepStrictEqual(h.problems, []);
            await h.close();
        }

        // The other way round: the stream is first with a change (it usually is, by about a second), and the REST API
        // then answers with the sensor's whole object, still holding the reading from before, because something
        // else in it has changed. That is not a reading: before, the stream's next message put it right, and now
        // nothing would. (2026-10-12 17:25:11Z looks like this: the reading from before was back 0.25 s after the
        // stream's new one, for 23 s, with no stream message in between.)
        {
            const h = await houses.open();
            const t = h.traits;
            await h.runTo(10);
            await h.rest('kryptonite.K1', { current_temperature: 20.3 });
            await h.runTo(600);
            await h.frame(t.temperature('K1', 19.6));
            assert.ok(near(h.shown('K1'), 19.6));
            await h.advance(50);
            await h.rest('kryptonite.K1', { battery_level: 99 });
            assert.ok(near(h.shown('K1'), 19.6), 'the REST API\'s unchanged reading does not replace the stream\'s newer one: ' + h.shown('K1'));
            assert.strictEqual(h.conn.currentState.kryptonite.K1.battery_level, 99, 'the rest of the object is taken');
            await h.advance(24000);
            await h.rest('kryptonite.K1', { current_temperature: 19.6 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 19.6));
            // The REST API's next change is one again, whatever the stream has
            await h.advance(60000);
            await h.rest('kryptonite.K1', { current_temperature: 19.1 });
            assert.ok(near(h.shown('K1'), 19.1));
            await h.frame(t.unrelated());
            // ... and so is a reading it had before, when it has had another in between
            await h.advance(60000);
            await h.rest('kryptonite.K1', { current_temperature: 19.6 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 19.6));
            nearAll(h.values('K1'), [20.3, 19.6, 19.1, 19.6], 'each reading once');
            // An object of a sensor the stream does not know, and one before the stream has delivered anything for
            // a sensor: taken as before, and the stream's own data is not added to
            assert.strictEqual(h.conn.protobufBody.kryptonite.K9, undefined);
            h.restObjects['kryptonite.K9'] = { object_key: 'kryptonite.K9', object_revision: 0, object_timestamp: 0, value: { current_temperature: 18.2, battery_level: 100 } };
            await h.rest('kryptonite.K9', {});
            await h.rest('kryptonite.K9', { current_temperature: 18.4 });
            assert.strictEqual(h.conn.currentState.kryptonite.K9.current_temperature, 18.4);
            assert.strictEqual(h.conn.protobufBody.kryptonite.K9, undefined, 'the stream\'s data has no sensor the stream did not deliver');
            assert.deepStrictEqual(h.problems, []);
            assert.deepStrictEqual(h.lines, []);
            await h.close();
        }

        // A new stream opens with Nest's full current state, and that is applied whatever came before: a reading the
        // REST API had first, one neither had (the change fell between two streams), and the same reading again
        {
            const h = await houses.open();
            const t = h.traits;
            await h.runTo(60);
            await h.rest('kryptonite.K1', { current_temperature: 20.8 });
            await h.rest('kryptonite.K3', { current_temperature: 19.1 });
            await h.runTo(100);
            await h.endStream();
            assert.ok(near(h.shown('K1'), 20.8));
            // (K1: the REST API's reading; K2: a change nobody has delivered; K3: Nest has moved on from REST's)
            Object.assign(h.state.K1, { temperature: 20.8 });
            Object.assign(h.state.K2, { temperature: 19.9 });
            Object.assign(h.state.K3, { temperature: 18.6 });
            await h.runTo(102);
            assert.strictEqual(h.streams.length, 2, 'the plugin has opened the next stream');
            await h.snapshot();
            assert.ok(near(h.shown('K1'), 20.8) && near(h.shown('K2'), 19.9) && near(h.shown('K3'), 18.6), 'the new stream\'s state is shown');
            await h.frame(t.unrelated());
            await h.rest('kryptonite.K3', { current_temperature: 18.6 });
            await h.rest('kryptonite.K2', { current_temperature: 19.9 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 20.8) && near(h.shown('K2'), 19.9) && near(h.shown('K3'), 18.6));
            nearAll(h.values('K1'), [20.8], 'K1');
            nearAll(h.values('K2'), [19.9], 'K2');
            nearAll(h.values('K3'), [19.1, 18.6], 'K3');
            // The stream's own later change of a reading the REST API was first with before
            await h.advance(60000);
            await h.frame(t.temperature('K1', 21.4));
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 21.4));
            await h.rest('kryptonite.K1', { current_temperature: 21.4 });
            assert.ok(near(h.shown('K1'), 21.4));
            assert.deepStrictEqual(h.problems, []);
            await h.close();
        }

        // What the stream's merge of everything is still needed for: the REST API's structure object replaces the
        // device list, which the stream's devices are in, and the stream's next message puts them back. And a REST
        // reading that is not a number is written as before (and the stream's put back as before).
        {
            const h = await houses.open();
            const t = h.traits;
            await h.runTo(30);
            await h.rest('structure.S1', { away: true });
            assert.ok(!h.conn.currentState.structure.S1.swarm.includes('device.T1'), 'the REST API\'s list has no thermostat');
            await h.frame(t.unrelated());
            assert.ok(h.conn.currentState.structure.S1.swarm.includes('device.T1'), 'the stream\'s next message puts them back');
            await h.frame(t.backplate('T1', 20.2), t.humidity('T1', 48.5));
            assert.ok(near(await h.read('T1', 'humidity'), 48.5));
            await h.rest('kryptonite.K2', { current_temperature: null });
            assert.strictEqual(h.conn.currentState.kryptonite.K2.current_temperature, null);
            assert.ok(near(h.shown('K2'), 20.4), 'HomeKit keeps the last reading');
            await h.frame(t.unrelated());
            assert.ok(near(h.conn.currentState.kryptonite.K2.current_temperature, 20.4));
            await h.rest('kryptonite.K2', { current_temperature: 20.4 });
            await h.rest('kryptonite.K2', { current_temperature: 20.1 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K2'), 20.1));
            assert.deepStrictEqual(h.problems, []);
            await h.close();
        }

        // A setpoint written while all this goes on is held and echoed as before
        {
            const h = await houses.open();
            const t = h.traits;
            await h.runTo(20);
            await h.write('T1', 'low', 20);
            await h.rest('kryptonite.K1', { current_temperature: 20.8 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('T1', 'low'), 20) && near(h.shown('K1'), 20.8));
            await h.runTo(23);
            assert.strictEqual(h.requests.length, 1, 'pushed after the debounce');
            assert.ok(near(h.requests[0].set[0].value.settings.targetTemperatureHeat.value, 20));
            await h.frame(t.temperature('K2', 20.6));
            await h.rest('kryptonite.K1', { battery_level: 98 });
            assert.ok(near(h.shown('T1', 'low'), 20), 'held while Nest still reports the value before it');
            assert.strictEqual(h.conn.mergeUpdates.length, 1);
            await h.runTo(26);
            await h.frame(t.setpoints('T1', { heat: 20, cool: 23 }));
            assert.strictEqual(h.conn.mergeUpdates.length, 0, 'released by Nest\'s echo');
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('T1', 'low'), 20) && near(h.shown('K1'), 20.8) && near(h.shown('K2'), 20.6));
            assert.deepStrictEqual(h.problems, []);
            assert.deepStrictEqual(h.lines.filter(line => line.level != 'info'), []);
            await h.close();
        }
    } finally {
        houses.dispose();
    }
};
