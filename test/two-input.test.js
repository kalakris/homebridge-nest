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

// The lines 4.6.10-ha.9 logs about a sensor's REST object (the house starts at START; the stream's values are float32)
const START = Date.UTC(2026, 9, 12, 17, 0, 0), f = Math.fround;
const fieldsLine = (id, more) => 'Sensor ..' + id + ': the REST API\'s object has the fields object_key (string), object_revision (number), object_timestamp (number), value (object); value: current_temperature (number), battery_level (number)' +
    (more === undefined ? ', where_id (string), serial_number (string), structure_id (string), last_updated_at (number)' : more) + '.';
const readingLine = (id, value, seconds, done, had, stream, revision, more) => 'Sensor ..' + id + ': the REST API\'s reading ' + value + ' at ' + new Date(START + seconds * 1000).toISOString() + ', ' + done +
    '; the current state had ' + had + ', the observe stream\'s copy ' + stream + '; object_revision ' + revision + ', object_timestamp ' + (START + seconds * 1000) + ', value.last_updated_at ' + START / 1000 + (more || '') + '.';
const startLines = () => [['K1', 21.3, 3], ['K2', 20.4, 4], ['K3', 19.6, 5]].reduce((lines, [id, value, revision]) => lines.concat([fieldsLine(id), readingLine(id, value, 0, 'written', 'nothing', 'nothing', revision)]), []);
const sensorLines = (h, id) => h.lines.filter(line => line.text.startsWith('Sensor ..' + id + ': the REST API\'s reading')).map(line => line.text);
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
            // The log: what the REST API's objects hold at the start, and its one reading since, with the plugin's
            // clock, what the current state and the stream's copy held, and the object's own numbers. All at info
            // level, and nothing else
            assert.deepStrictEqual(h.lines.map(line => line.text), startLines().concat([
                readingLine('K1', 20.8, 60, 'written', f(21.3), f(21.3), 6)
            ]));
            assert.ok(h.lines.every(line => line.level == 'info'));
            // (Of the object's texts, its serial number, room, structure and user, none is in a line)
            assert.ok(h.lines.every(line => !/SERIAL|WK\d|\bS1\b|user/.test(line.text)));
            assert.strictEqual(h.lines[6].text, 'Sensor ..K1: the REST API\'s reading 20.8 at 2026-10-12T17:01:00.000Z, written; the current state had 21.299999237060547, the observe stream\'s copy 21.299999237060547; ' +
                'object_revision 6, object_timestamp 1791824460000, value.last_updated_at 1791824400.');
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
            // (The object twice more with nothing new, and nothing to put right: counted, not logged. The second time
            // after the stream has delivered the same reading, which it does as a float32)
            await h.advance(1000);
            await h.rest('kryptonite.K1', { battery_level: 98 });
            await h.advance(500);
            await h.frame(t.temperature('K1', 19.1));
            assert.strictEqual(h.conn.currentState.kryptonite.K1.current_temperature, f(19.1));
            await h.advance(500);
            await h.rest('kryptonite.K1', { battery_level: 97 });
            assert.ok(near(h.shown('K1'), 19.1));
            // ... and so is a reading it had before, when it has had another in between
            await h.advance(58000);
            await h.rest('kryptonite.K1', { current_temperature: 19.6 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('K1'), 19.6));
            nearAll(h.values('K1'), [20.3, 19.6, 19.1, 19.6], 'each reading once');
            // An object of a sensor the stream does not know, and one before the stream has delivered anything for
            // a sensor: taken as before, and the stream's own data is not added to
            assert.strictEqual(h.conn.protobufBody.kryptonite.K9, undefined);
            // (Its times are texts, and it has an id that is all digits: a text is logged only if it is a number or a date
            // and time, and its field is named like a time)
            h.restObjects['kryptonite.K9'] = { object_key: 'kryptonite.K9', object_revision: 0, object_timestamp: 0,
                value: { current_temperature: 18.2, battery_level: 100, serial_number: '1234567890', last_updated_at: '2026-10-12T17:12:24.150Z', report_time: '1791825144', updated_by: 'user.12345' } };
            await h.rest('kryptonite.K9', {});
            await h.rest('kryptonite.K9', { current_temperature: 18.4 });
            assert.strictEqual(h.conn.currentState.kryptonite.K9.current_temperature, 18.4);
            assert.strictEqual(h.conn.protobufBody.kryptonite.K9, undefined, 'the stream\'s data has no sensor the stream did not deliver');
            assert.deepStrictEqual(h.problems, []);
            // The log: a line for each reading that was news, one for the reading that was not and differed from what
            // the current state held, none for the two that changed nothing (the next line counts them)
            assert.deepStrictEqual(sensorLines(h, 'K1'), [
                readingLine('K1', 21.3, 0, 'written', 'nothing', 'nothing', 3),
                readingLine('K1', 20.3, 10, 'written', f(21.3), f(21.3), 6),
                readingLine('K1', 20.3, 600.05, 'not written (the REST API\'s last again)', f(19.6), f(19.6), 7),
                readingLine('K1', 19.6, 624.05, 'written', f(19.6), f(19.6), 8),
                readingLine('K1', 19.1, 684.05, 'written', 19.6, 19.6, 9),
                readingLine('K1', 19.6, 744.05, 'written', f(19.1), f(19.1), 12, '; 2 answer(s) with the reading before it again since the last line, the last with object_revision 11, object_timestamp ' + (START + 686050) +
                    ', value.last_updated_at ' + START / 1000)
            ]);
            const unknown = h.lines.filter(line => /^Sensor \.\.K9: /.test(line.text)).map(line => line.text.replace(/ at \S+,/, ' at T,').replace(/object_timestamp \d+/, 'object_timestamp T'));
            assert.deepStrictEqual(unknown, [
                fieldsLine('K9', ', serial_number (string), last_updated_at (string), report_time (string), updated_by (string)'),
                'Sensor ..K9: the REST API\'s reading 18.2 at T, written; the current state had nothing, the observe stream\'s copy nothing; object_revision 13, object_timestamp T, ' +
                    'value.last_updated_at 2026-10-12T17:12:24.150Z, value.report_time 1791825144.',
                'Sensor ..K9: the REST API\'s reading 18.4 at T, written; the current state had 18.2, the observe stream\'s copy nothing; object_revision 14, object_timestamp T, ' +
                    'value.last_updated_at 2026-10-12T17:12:24.150Z, value.report_time 1791825144.'
            ]);
            assert.strictEqual(h.lines.length, 6 + 5 + 3);
            assert.ok(h.lines.every(line => !/SERIAL|WK\d|\bS1\b|user|1234567890/.test(line.text)), 'no text of an object that is not a number or a time in a field named like one');
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
            // A second write: Nest's answer to each is in the log (before 4.6.10-ha.9 only the first of a kind)
            await h.write('T1', 'low', 20.5);
            await h.runTo(30);
            await h.frame(t.setpoints('T1', { heat: 20.5, cool: 23 }));
            assert.strictEqual(h.conn.mergeUpdates.length, 0);
            assert.deepStrictEqual(h.lines.filter(line => /^Nest answered/.test(line.text)).map(line => line.level + ' ' + line.text), [
                'info Nest answered the change shared.T1 {"target_temperature_low":20} after 0.3 s: HTTP 200, 0 byte(s), empty.',
                'info Nest answered the change shared.T1 {"target_temperature_low":20.5} after 0.3 s: HTTP 200, 0 byte(s), empty.'
            ]);
            assert.deepStrictEqual(h.problems, []);
            assert.deepStrictEqual(h.lines.filter(line => line.level != 'info'), []);
            await h.close();
        }

        // The lines about the REST API's readings and about Nest's answers decide nothing: with a logger that throws
        // at each of them everything is as without. And a sensor whose reading changes all the time does not fill
        // the log: 30 lines in an hour, and the next line says how many were left out.
        {
            const h = await houses.open();
            const t = h.traits;
            h.conn.evidence = () => {
                throw new Error('the logger failed');
            };
            await h.runTo(20);
            await h.write('T1', 'low', 20);
            await h.rest('kryptonite.K1', { current_temperature: 20.8 });
            await h.frame(t.unrelated());
            await h.runTo(26);
            await h.frame(t.setpoints('T1', { heat: 20, cool: 23 }));
            await h.write('T1', 'low', 20.5);
            await h.runTo(32);
            await h.frame(t.setpoints('T1', { heat: 20.5, cool: 23 }));
            await h.rest('kryptonite.K1', { battery_level: 90 });
            await h.rest('kryptonite.K1', { current_temperature: 20.3 });
            await h.frame(t.unrelated());
            assert.ok(near(h.shown('T1', 'low'), 20.5) && near(h.shown('K1'), 20.3));
            assert.strictEqual(h.requests.length, 2);
            assert.strictEqual(h.conn.mergeUpdates.length, 0);
            assert.deepStrictEqual(h.problems, []);
            assert.strictEqual(h.lines.filter(line => /^Sensor /.test(line.text)).length, 6, 'only the start\'s, logged before the logger was replaced');
            await h.close();
        }
        {
            const h = await houses.open();
            await h.runTo(10);
            for (let i = 0; i < 40; i++) {
                await h.advance(10000);
                await h.rest('kryptonite.K3', { current_temperature: 19 + (i % 2) / 10 });
            }
            assert.strictEqual(h.lines.filter(line => /^Sensor \.\.K3: /.test(line.text)).length, 30);
            assert.strictEqual(sensorLines(h, 'K3').length, 29);
            assert.ok(near(h.shown('K3'), 19.1), 'every reading was taken');
            await h.advance(3600000);
            await h.rest('kryptonite.K3', { current_temperature: 18.5 });
            const last = sensorLines(h, 'K3').pop();
            assert.ok(/reading 18\.5 at .*, written; the current state had 19\.1, the observe stream's copy 19\.1; .*; 12 line\(s\) left out\.$/.test(last), last);
            assert.strictEqual(h.lines.filter(line => /^Sensor \.\.K[12]: /.test(line.text)).length, 4);
            assert.deepStrictEqual(h.problems, []);
            await h.close();
        }
    } finally {
        houses.dispose();
    }
};
