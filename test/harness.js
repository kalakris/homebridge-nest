'use strict';

// Minimal Homebridge stand-in built on the real HAP-NodeJS, so tests see the same
// validation and wire rounding HomeKit controllers (Home Assistant) get.

const hap = require('hap-nodejs');
const { formatOutgoingCharacteristicValue } = require('hap-nodejs/dist/lib/util/request-util');

class PlatformAccessory {
    constructor(name, uuid) {
        this.displayName = name;
        this.UUID = uuid;
        this._associatedHAPAccessory = new hap.Accessory(name, uuid);
    }
}

const exportedTypes = {
    Accessory: PlatformAccessory,
    Service: hap.Service,
    Characteristic: hap.Characteristic,
    hap: hap,
    uuid: hap.uuid
};

require('../lib/nest-device-accessory')(exportedTypes);
const ThermostatAccessory = require('../lib/nest-thermostat-accessory')();
const TempSensorAccessory = require('../lib/nest-tempsensor-accessory')();

function makePlatform(options, conn) {
    return {
        config: { options: options || [] },
        conn: conn,
        optionSet(key, serialNumber, deviceId) {
            return key && this.config.options && (this.config.options.includes(key) || (serialNumber && this.config.options.includes(key + '.' + serialNumber)) || (deviceId && this.config.options.includes(key + '.' + deviceId)));
        }
    };
}

function makeConn(overrides) {
    return Object.assign({
        verbose() {},
        isStale() { return false; },
        update() { return Promise.resolve(true); }
    }, overrides || {});
}

const silentLog = Object.assign(function() {}, { debug() {}, info() {}, warn() {}, error() {} });

function thermostatDevice(overrides) {
    return Object.assign({
        device_id: 'T1', serial_number: 'SERIALT1', name: 'Upstairs Thermostat', where_name: 'Upstairs',
        structure_id: 'S1', using_protobuf: true, temperature_scale: 'F', can_heat: true, can_cool: true,
        has_fan: false, has_eco_mode: false, hvac_mode: 'range', previous_hvac_mode: 'range',
        target_temperature_type: 'range', hvac_state: 'off', is_online: true,
        current_temperature: 20.37, backplate_temperature: 19.81, current_humidity: 55.37,
        target_temperature_low: 20.5556, target_temperature_high: 22.2222, target_temperature: 21.3889,
        has_temperature_sensors: true
    }, overrides || {});
}

function sensorDevice(overrides) {
    return Object.assign({
        device_id: 'K1', serial_number: 'SERIALK1', name: 'Nursery', structure_id: 'S1',
        thermostat_device_id: 'T1', temperature_scale: 'F', current_temperature: 20.37,
        battery_voltage: 3, using_protobuf: true
    }, overrides || {});
}

const structure = { structure_id: 'S1' };

function makeThermostat(device, options, conn) {
    conn = conn || makeConn();
    return new ThermostatAccessory(conn, silentLog, thermostatDevice(device), structure, makePlatform(options, conn));
}

function makeSensor(device, options, conn) {
    conn = conn || makeConn();
    return new TempSensorAccessory(conn, silentLog, sensorDevice(device), structure, makePlatform(options, conn));
}

// What a HomeKit controller reads: the get handler, HAP validation, then HAP's wire formatting.
async function readWire(service, characteristicType) {
    const characteristic = service.getCharacteristic(characteristicType);
    const value = await characteristic.handleGetRequest();
    return formatOutgoingCharacteristicValue(value, characteristic.props);
}

module.exports = { hap, makeThermostat, makeSensor, makeConn, makePlatform, readWire, silentLog, thermostatDevice, sensorDevice, structure };
