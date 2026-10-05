'use strict';

const assert = require('assert');
const { hap, makeThermostat, makeSensor, readWire } = require('./harness');
const { Service, Characteristic } = hap;

const cToF = c => c * 1.8 + 32;
const close = (a, b, msg) => assert.ok(Math.abs(a - b) < 1e-9, msg + ': ' + a + ' != ' + b);

module.exports = async function() {
    // Default: unrounded, on a 0.01 degC grid
    for (const scale of ['F', 'C']) {
        const t = makeThermostat({ temperature_scale: scale });
        const svc = t.getService(Service.Thermostat);
        close(await readWire(svc, Characteristic.CurrentTemperature), 20.37, 'thermostat current, ' + scale);
        close(await readWire(svc, Characteristic.CurrentRelativeHumidity), 55.37, 'humidity, ' + scale);
        const backplate = t.accessory._associatedHAPAccessory.services.find(s => s.UUID === Service.TemperatureSensor.UUID);
        close(await readWire(backplate, Characteristic.CurrentTemperature), 19.81, 'backplate, ' + scale);

        const s = makeSensor({ temperature_scale: scale });
        close(await readWire(s.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 20.37, 'sensor, ' + scale);
    }

    // float32 values from the protobuf API land on the 0.01 grid
    {
        const s = makeSensor({ current_temperature: Math.fround(21.62) });
        close(await readWire(s.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 21.62, 'float32 sensor value');
        const s2 = makeSensor({ current_temperature: 21.5625 });
        close(await readWire(s2.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 21.56, 'sub-grid value rounds to 0.01');
    }

    // Temperature.Round.Enable: upstream behaviour, including its props (Fahrenheit grid offset by -17.78 degC)
    {
        const t = makeThermostat({ temperature_scale: 'F' }, ['Temperature.Round.Enable']);
        const svc = t.getService(Service.Thermostat);
        const wire = await readWire(svc, Characteristic.CurrentTemperature);
        // 20.37 degC = 68.67 degF -> 69 degF = 20.5556 degC -> 0.1 grid from -17.7778 -> 20.5222 degC (68.94 degF)
        close(wire, 20.5222, 'rounded thermostat, F');
        close(Math.round(cToF(wire) * 100) / 100, 68.94, 'rounded thermostat in degF');
        assert.strictEqual(svc.getCharacteristic(Characteristic.CurrentTemperature).props.minStep, 0.1);
        assert.strictEqual(svc.getCharacteristic(Characteristic.CurrentRelativeHumidity).props.minStep, 1);
        close(await readWire(svc, Characteristic.CurrentRelativeHumidity), 55, 'rounded humidity');

        const tc = makeThermostat({ temperature_scale: 'C' }, ['Temperature.Round.Enable']);
        close(await readWire(tc.getService(Service.Thermostat), Characteristic.CurrentTemperature), 20.5, 'rounded thermostat, C');

        const s = makeSensor({ temperature_scale: 'F' }, ['Temperature.Round.Enable']);
        close(await readWire(s.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 20.5222, 'rounded sensor, F');

        // per-device suffix
        const s2 = makeSensor({ temperature_scale: 'F' }, ['Temperature.Round.Enable.OTHERSERIAL']);
        close(await readWire(s2.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 20.37, 'option for another device');
        const s3 = makeSensor({ temperature_scale: 'F' }, ['Temperature.Round.Enable.SERIALK1']);
        close(await readWire(s3.getService(Service.TemperatureSensor), Characteristic.CurrentTemperature), 20.5222, 'option for this device');
    }

    // Setpoints keep upstream behaviour: whole degF, then the 0.1 degC grid from 10 degC
    {
        const t = makeThermostat({ temperature_scale: 'F' });
        const svc = t.getService(Service.Thermostat);
        close(await readWire(svc, Characteristic.HeatingThresholdTemperature), 20.6, 'heating threshold (69 degF)');
        close(await readWire(svc, Characteristic.CoolingThresholdTemperature), 22.2, 'cooling threshold (72 degF)');
        const off = makeThermostat({ temperature_scale: 'F', hvac_mode: 'off' });
        // off: target = current temperature rounded like a setpoint (69 degF)
        close(await readWire(off.getService(Service.Thermostat), Characteristic.TargetTemperature), 20.6, 'target in off mode');
    }
};
