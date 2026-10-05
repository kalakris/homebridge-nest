/**
 * Created by kraig on 3/11/16.
 */

const inherits = require('util').inherits;
let Accessory, Service, Characteristic, uuid, hap;

// Resolution of measured temperatures (deg C) and humidity (%) reported to HomeKit. HAP-NodeJS rounds
// outgoing values to minStep and then to 4 decimals; Home Assistant's homekit_controller passes reads through.
const CURRENT_TEMPERATURE_MIN_STEP = 0.01;
const CURRENT_HUMIDITY_MIN_STEP = 0.01;

'use strict';

module.exports = function(exportedTypes) {
    if (exportedTypes && !Accessory) {
        Accessory = exportedTypes.Accessory;
        Service = exportedTypes.Service;
        Characteristic = exportedTypes.Characteristic;
        uuid = exportedTypes.uuid;
        hap = exportedTypes.hap;

        const acc = NestDeviceAccessory.prototype;
        inherits(NestDeviceAccessory, Accessory);
        NestDeviceAccessory.prototype.parent = Accessory.prototype;
        for (const mn in acc) {
            NestDeviceAccessory.prototype[mn] = acc[mn];
        }
    }
    return {
        NestDeviceAccessory: NestDeviceAccessory,
        Accessory: Accessory,
        Service: Service,
        Characteristic: Characteristic,
        hap: hap
    };
};

// Base type for Nest devices
function NestDeviceAccessory(conn, log, device, structure, platform) {

    // device info
    this.conn = conn;
    this.name = this.homeKitSanitize(device.name_long || device.name);
    this.deviceId = device.device_id;
    this.log = log;
    this.device = device;
    this.structure = structure;
    this.structureId = structure.structure_id;
    this.platform = platform;

    this.log('initing ' + this.deviceType + (device.using_protobuf ? ' (P)' : '') + ' "' + this.name + '":', 'deviceId:', this.deviceId, 'structureId:', this.structureId);
    // this.log.debug(this.device);

    const id = uuid.generate('nest' + '.' + this.deviceType + '.' + this.deviceId);
    this.accessory = new Accessory(this.name, id);
    this.uuid_base = id;

    this.addService = this.accessory._associatedHAPAccessory.addService.bind(this.accessory._associatedHAPAccessory);
    this.getService = this.accessory._associatedHAPAccessory.getService.bind(this.accessory._associatedHAPAccessory);
    // this.setPrimaryService = this.accessory._associatedHAPAccessory.setPrimaryService.bind(this.accessory._associatedHAPAccessory);

    this.getService(Service.AccessoryInformation)
        .setCharacteristic(Characteristic.FirmwareRevision, this.device.software_version || '1.0')
        .setCharacteristic(Characteristic.Manufacturer, 'Nest')
        .setCharacteristic(Characteristic.Model, this.device.model || this.device.model_name || this.deviceDesc)
        .setCharacteristic(Characteristic.Name, this.name)
        .setCharacteristic(Characteristic.SerialNumber, this.device.serial_number || this.device.device_id || 'None');

    this.boundCharacteristics = [];

    // this.updateData();
}

NestDeviceAccessory.prototype.getServices = function () {
    return this.services;
};

NestDeviceAccessory.prototype.bindCharacteristic = function (service, characteristic, desc, getFunc, setFunc, format) {
    const actual = service.getCharacteristic(characteristic)
        .on('get', function (callback) {
            if (this.isStale()) {
                if (callback) callback(communicationFailure());
                return;
            }
            const val = getFunc.bind(this)();
            if (callback) callback(null, val);
        }.bind(this))
        .on('change', function (change) {
            if (change.oldValue === change.newValue) {
                return;
            }
            let disp = change.newValue;
            if (format && disp !== null) {
                disp = format.call(this, disp);
            }
            this.log.debug(desc + ' for ' + this.name + ' is: ' + disp);
        }.bind(this));
    if (setFunc) {
        actual.on('set', function (value, callback) {
            // Refuse before the setter touches any local state
            const blockedReason = this.conn && this.conn.writeBlockedReason && this.conn.writeBlockedReason();
            if (blockedReason) {
                this.log.error('Not setting ' + desc + ' for ' + this.name + ' to ' + value + ': ' + blockedReason + '.');
                callback(communicationFailure());
                return;
            }
            setFunc.call(this, value, callback);
        }.bind(this));
    }
    this.boundCharacteristics.push([service, characteristic, getFunc]);
};

// No data from Nest for staleDataTimeoutMinutes (see Connection.isStale)
NestDeviceAccessory.prototype.isStale = function () {
    return !!(this.conn && this.conn.isStale && this.conn.isStale());
};

function communicationFailure() {
    if (hap && hap.HapStatusError && hap.HAPStatus) {
        return new hap.HapStatusError(hap.HAPStatus.SERVICE_COMMUNICATION_FAILURE);
    }
    return new Error('No current data from Nest');
}

NestDeviceAccessory.prototype.fahrenheitToCelsius = function(temperature) {
    return (temperature - 32) / 1.8;
};

NestDeviceAccessory.prototype.celsiusToFahrenheit = function(temperature) {
    return (temperature * 1.8) + 32;
};

// Rounds to the unit the Nest displays: whole degF for Fahrenheit users, 0.5 degC for Celsius users.
// Setpoints are always reported this way (the Nest only accepts those steps). Measured temperatures
// are only rounded when the Temperature.Round.Enable option is set (upstream behaviour, see #321).
NestDeviceAccessory.prototype.roundToDisplayUnits = function(temperature) {
    if (this.usesFahrenheit && this.usesFahrenheit()) {
        // Uses deg F? Round to nearest degree in F.
        let tempF = Math.round(this.celsiusToFahrenheit(temperature));
        return this.fahrenheitToCelsius(tempF);
    } else if (this.usesFahrenheit && !this.usesFahrenheit()) {
        // Uses deg C? Round to nearest half degree in C.
        let tempC = 0.5 * Math.round(2 * temperature);
        return tempC;
    } else {
        return temperature;
    }
};

NestDeviceAccessory.prototype.roundTemperatures = function() {
    return !!this.platform.optionSet('Temperature.Round.Enable', this.device.serial_number, this.device.device_id);
};

// Measured temperature (deg C) as reported to HomeKit: the backend value unrounded, unless rounding is enabled.
NestDeviceAccessory.prototype.reportTemperature = function(temperature) {
    if (temperature === undefined || temperature === null) {
        return temperature;
    }
    return this.roundTemperatures() ? this.roundToDisplayUnits(temperature) : temperature;
};

// Characteristic props for a measured temperature. HAP-NodeJS snaps every value to minValue + k * minStep
// (and to 4 decimals on the wire), so minValue must be a round number or the grid is offset
// (the upstream -17.78 degC minimum produced values like 20.0222 degC = 68.04 degF).
NestDeviceAccessory.prototype.currentTemperatureProps = function() {
    if (this.roundTemperatures()) {
        // Upstream props, unchanged
        if (this.usesFahrenheit()) {
            return { minStep: 0.1, minValue: this.fahrenheitToCelsius(0), maxValue: this.fahrenheitToCelsius(160) };
        }
        return { minStep: 0.1, minValue: -20, maxValue: 60 };
    }
    return { minStep: CURRENT_TEMPERATURE_MIN_STEP, minValue: -20, maxValue: this.usesFahrenheit() ? this.fahrenheitToCelsius(160) : 60 };
};

// Characteristic props for a measured relative humidity (HAP default minStep is 1).
NestDeviceAccessory.prototype.currentHumidityProps = function() {
    return this.roundTemperatures() ? {} : { minStep: CURRENT_HUMIDITY_MIN_STEP };
};

NestDeviceAccessory.prototype.updateData = function (device, structure) {
    if (device) {
        this.device = device;
    }
    if (structure) {
        this.structure = structure;
    }
    if (this.isStale()) {
        // An event would mark the characteristic healthy again in the controller until its next read
        return;
    }
    // Push the new values to HomeKit, which sends event notifications to subscribed controllers
    // for the ones that changed. (This used to read .value, a no-op since HAP-NodeJS dropped the
    // getValue() side effect, so controllers only saw changes when they polled - every 60 s for
    // Home Assistant.)
    this.boundCharacteristics.forEach(function (c) {
        let value;
        try {
            value = c[2].call(this);
        } catch (error) {
            this.log.error('Unable to compute ' + c[1].name + ' for ' + this.name + ':', error);
            return;
        }
        if (value !== undefined && value !== null) {
            c[0].getCharacteristic(c[1]).updateValue(value);
        }
    }.bind(this));
};

NestDeviceAccessory.prototype.setPropertyAsync = function(type, property, value, propertyDescription, valueDescription, doNotUpdateLocalProperty) {
    propertyDescription = propertyDescription || property;
    valueDescription = valueDescription || value;
    const blockedReason = this.conn.writeBlockedReason && this.conn.writeBlockedReason();
    if (blockedReason) {
        // Fail the HomeKit write rather than accept a change that cannot reach Nest (and would silently revert)
        this.log.error('Not setting ' + propertyDescription + ' for ' + this.name + ' to ' + valueDescription + ': ' + blockedReason + '.');
        return Promise.reject(communicationFailure());
    }
    this.log.debug('Setting ' + propertyDescription + ' for ' + this.name + ' to: ' + valueDescription);
    if (!doNotUpdateLocalProperty) {
        this.device[property] = value;
    }

    switch (type) {
    case 'structure': // Structure
        return this.conn.update(type + '.' + this.structureId, property, value, this.isZirconium ? this.device.previous_hvac_mode : this.device.hvac_mode, false);
    case 'device':    // Thermostat
    case 'shared':    // Thermostat
    case 'topaz':     // Protect
    case 'kryptonite':     // Temperature Sensor
    case 'quartz':    // Camera
    case 'yale':      // Lock
        return this.conn.update(type + '.' + this.deviceId, property, value, this.isZirconium ? this.device.previous_hvac_mode : this.device.hvac_mode, this.device.using_protobuf);
    default:
        this.log.debug('Could not set property - unknown type: ' + type);
        return Promise.resolve(null);
    }
};

NestDeviceAccessory.prototype.homeKitSanitize = function(name) {
    // Returns a name containing only allowed HomeKit device name characters

    let fixedName = name.replace(/[^A-Za-z0-9 '-]/g, '') || 'Unnamed';
    return fixedName;
};
