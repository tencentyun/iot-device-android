/*
 * Copyright 2017, The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.iot.hub.device.android.app;

import android.bluetooth.BluetoothGattCharacteristic;
import android.bluetooth.BluetoothGattDescriptor;
import android.bluetooth.BluetoothGattService;

import java.util.Calendar;
import java.util.UUID;

/**
 * Implementation of the Bluetooth GATT Time Profile.
 * https://www.bluetooth.com/specifications/adopted-specifications
 */
public class IOTBLEProfile {
    private static final String TAG = IOTBLEProfile.class.getSimpleName();

    /* IOT BLE Service UUID */
    public static UUID IOT_BLE_SERVICE     = UUID.fromString("0000fff0-65d0-4e20-b56a-e493541ba4e2");
    /* IOT BLE DEVICE INFO Characteristic UUID */
    public static UUID IOT_BLE_DEVICE_INFO = UUID.fromString("0000ffe1-65d0-4e20-b56a-e493541ba4e2");
    /* IOT BLE EVENT Characteristic UUID */
    public static UUID IOT_BLE_EVENT       = UUID.fromString("0000ffe3-65d0-4e20-b56a-e493541ba4e2");
    /* IOT BLE EVENT Characteristic UUID */
    public static UUID CLIENT_CONFIG       = UUID.fromString("00000292-65d0-4e20-b56a-e493541ba4e2");

    // Adjustment Flags
    public static final byte ADJUST_NONE     = 0x0;
    public static final byte ADJUST_MANUAL   = 0x1;
    public static final byte ADJUST_EXTERNAL = 0x2;
    public static final byte ADJUST_TIMEZONE = 0x4;
    public static final byte ADJUST_DST      = 0x8;

    /**
     * Return a configured {@link BluetoothGattService} instance for the
     * Current Time Service.
     */
    public static BluetoothGattService createIOTBLEService() {
        BluetoothGattService service = new BluetoothGattService(IOT_BLE_SERVICE,
                BluetoothGattService.SERVICE_TYPE_PRIMARY);

        // IOT Device Info characteristic
        BluetoothGattCharacteristic deviceinfo = new BluetoothGattCharacteristic(IOT_BLE_DEVICE_INFO,
                //Read-only characteristic, supports notifications
                BluetoothGattCharacteristic.PROPERTY_WRITE | BluetoothGattCharacteristic.PROPERTY_READ,
                BluetoothGattCharacteristic.PERMISSION_WRITE | BluetoothGattCharacteristic.PERMISSION_READ);

        // IOT Event characteristic
        BluetoothGattCharacteristic event = new BluetoothGattCharacteristic(IOT_BLE_EVENT,
                //Read-only characteristic
                BluetoothGattCharacteristic.PROPERTY_NOTIFY,
                BluetoothGattCharacteristic.PERMISSION_READ);
        BluetoothGattDescriptor configDescriptor = new BluetoothGattDescriptor(CLIENT_CONFIG,
                //Read/write descriptor
                BluetoothGattDescriptor.PERMISSION_READ);
        event.addDescriptor(configDescriptor);

        service.addCharacteristic(deviceinfo);
        service.addCharacteristic(event);

        return service;
    }

    /**
     * Construct the field values for a Current Time characteristic
     * from the given epoch timestamp and adjustment reason.
     */
    public static byte[] getExactTime(long timestamp, byte adjustReason) {
        Calendar time = Calendar.getInstance();
        time.setTimeInMillis(timestamp);

        byte[] field = new byte[10];

        // Year
        int year = time.get(Calendar.YEAR);
        field[0] = (byte) (year & 0xFF);
        field[1] = (byte) ((year >> 8) & 0xFF);
        // Month
        field[2] = (byte) (time.get(Calendar.MONTH) + 1);
        // Day
        field[3] = (byte) time.get(Calendar.DATE);
        // Hours
        field[4] = (byte) time.get(Calendar.HOUR_OF_DAY);
        // Minutes
        field[5] = (byte) time.get(Calendar.MINUTE);
        // Seconds
        field[6] = (byte) time.get(Calendar.SECOND);
        // Day of Week (1-7)
        field[7] = getDayOfWeekCode(time.get(Calendar.DAY_OF_WEEK));
        // Fractions256
        field[8] = (byte) (time.get(Calendar.MILLISECOND) / 256);

        field[9] = adjustReason;

        return field;
    }

    /* Time bucket constants for local time information */
    private static final int FIFTEEN_MINUTE_MILLIS = 900000;
    private static final int HALF_HOUR_MILLIS = 1800000;

    /**
     * Construct the field values for a Local Time Information characteristic
     * from the given epoch timestamp.
     */
    public static byte[] getLocalTimeInfo(long timestamp) {
        Calendar time = Calendar.getInstance();
        time.setTimeInMillis(timestamp);

        byte[] field = new byte[2];

        // Time zone
        int zoneOffset = time.get(Calendar.ZONE_OFFSET) / FIFTEEN_MINUTE_MILLIS; // 15 minute intervals
        field[0] = (byte) zoneOffset;

        // DST Offset
        int dstOffset = time.get(Calendar.DST_OFFSET) / HALF_HOUR_MILLIS; // 30 minute intervals
        field[1] = getDstOffsetCode(dstOffset);

        return field;
    }

    /* Bluetooth Weekday Codes */
    private static final byte DAY_UNKNOWN = 0;
    private static final byte DAY_MONDAY = 1;
    private static final byte DAY_TUESDAY = 2;
    private static final byte DAY_WEDNESDAY = 3;
    private static final byte DAY_THURSDAY = 4;
    private static final byte DAY_FRIDAY = 5;
    private static final byte DAY_SATURDAY = 6;
    private static final byte DAY_SUNDAY = 7;

    /**
     * Convert a {@link Calendar} weekday value to the corresponding
     * Bluetooth weekday code.
     */
    private static byte getDayOfWeekCode(int dayOfWeek) {
        switch (dayOfWeek) {
            case Calendar.MONDAY:
                return DAY_MONDAY;
            case Calendar.TUESDAY:
                return DAY_TUESDAY;
            case Calendar.WEDNESDAY:
                return DAY_WEDNESDAY;
            case Calendar.THURSDAY:
                return DAY_THURSDAY;
            case Calendar.FRIDAY:
                return DAY_FRIDAY;
            case Calendar.SATURDAY:
                return DAY_SATURDAY;
            case Calendar.SUNDAY:
                return DAY_SUNDAY;
            default:
                return DAY_UNKNOWN;
        }
    }

    /* Bluetooth DST Offset Codes */
    private static final byte DST_STANDARD = 0x0;
    private static final byte DST_HALF     = 0x2;
    private static final byte DST_SINGLE   = 0x4;
    private static final byte DST_DOUBLE   = 0x8;
    private static final byte DST_UNKNOWN = (byte) 0xFF;

    /**
     * Convert a raw DST offset (in 30 minute intervals) to the
     * corresponding Bluetooth DST offset code.
     */
    private static byte getDstOffsetCode(int rawOffset) {
        switch (rawOffset) {
            case 0:
                return DST_STANDARD;
            case 1:
                return DST_HALF;
            case 2:
                return DST_SINGLE;
            case 4:
                return DST_DOUBLE;
            default:
                return DST_UNKNOWN;
        }
    }
}
