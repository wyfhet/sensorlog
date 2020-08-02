#!/usr/bin/env python
# rflog_db.py Interface between RF Module serial interface and AdafruitIO
# ---------------------------------------------------------------------------------
# J. Evans May 2018
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Revision History
# V1.00 - Release
# V2.00 - Updates to improve performance and added new sensor types
# -----------------------------------------------------------------------------------
#
import serial
import time
from time import sleep
import sys
from threading import Thread
import _thread
from influxdb import InfluxDBClient
from bme280 import process_bme_reading
from rf2serial import rf2serial
import rfsettings
import datetime
DEBUG = True
Farenheit = False


def dprint(message):
    if (DEBUG):
        print
        message


def ProcessMessageThread(value, value2, DevId, type):
    try:
        _thread.start_new_thread(ProcessMessage, (value, value2, DevId, type,))
    except:
        print
        "Error: unable to start thread"


def LogTelemetry(devid, type, value, uom):
    dprint("loggin entry")
    current_time = datetime.datetime.now()
    json_body = [{
        "measurement": type,
        "tags": {
            "uom": uom,
            "deviceId": devid
        },
        "time": current_time,
        "fields": {
            "value": value
        }}];
    # log the temperature to the database
    # open database connection
    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('luke_temparture_and__humidity')
    client.write_points(json_body);
    dprint("Telemetry " + str(devid) + "," + str(type) + "," + str(value) + "," + uom + "," + "logged")


def ProcessMessage(value, DevId, type, uom):
    # Notify the host that there is new data from a sensor (e.g. door open)
    try:
        dprint("Processing data : DevId=" + str(DevId) + ",Type=" + str(type) + ",Value=" + str(value))
        LogTelemetry(DevId, type, value, uom)

    except Exception as e:
        dprint(e)
    return (0)


def remove_duplicates():
    x = 0
    print
    "sorted deduplified queue:"

    # sort the queue by ID
    rfsettings.message_queue = sorted(rfsettings.message_queue, key=lambda x: (x[0]))

    x = 0
    while x < len(rfsettings.message_queue) - 1:
        if rfsettings.message_queue[x][0] == rfsettings.message_queue[x + 1][0] and \
                rfsettings.message_queue[x][1] == rfsettings.message_queue[x + 1][1]:
            rfsettings.message_queue.pop(x)
        else:
            x = x + 1

    for x in range(0, len(rfsettings.message_queue)):
        print
        rfsettings.message_queue[x][0] + rfsettings.message_queue[x][1]


def queue_processing():
    global measure
    try:
        sensordata = ""
        bme_data = ""
        bme_messages = 0
        uom = ""
        start_time = time.time()
        while (True):
            if len(rfsettings.message_queue) > 0 and not rfsettings.rf_event.is_set():
                remove_duplicates()
                message = rfsettings.message_queue.pop()
                devID = message[0]
                data = message[1]
                dprint(time.strftime("%c") + " " + message[0] + message[1])
                if data.startswith('BUTTONON'):
                    sensordata = 0
                    db_type = 1
                    uom = ""

                if data.startswith('STATEON'):
                    sensordata = 0
                    db_type = 2
                    uom = ""

                if data.startswith('STATEOFF'):
                    sensordata = 1
                    db_type = 2
                    uom = ""

                if data.startswith('BUTTONOFF'):
                    sensordata = 1
                    db_type = 1
                    uom = ""

                if data.startswith('TMPA'):
                    sensordata = DoFahrenheitConversion(str(data[4:].rstrip("-")))
                    db_type = 3
                    if Farenheit:
                        uom = "F"
                    else:
                        uom = "C"

                if data.startswith('ANAA'):
                    sensordata = str(data[4:].rstrip("-"))
                    sensordata = (float(
                        sensordata) - 1470) / 16  # convert it to a reading between 1(light) and 48 (dark)
                    sensordata = str(sensordata)
                    db_type = 4
                    measure = '2'
                    uom = ""

                if data.startswith('ANAB'):
                    sensordata = str(data[4:].rstrip("-"))
                    sensordata = (float(
                        sensordata) - 1470) / 16  # convert it to a reading between 1(light) and 48 (dark)
                    sensordata = str(sensordata)
                    measure = '2'
                    db_type = 4
                    uom = ""

                if data.startswith('TMPC'):
                    sensordata = DoFahrenheitConversion(str(data[4:].rstrip("-")))
                    db_type = 1
                    if Farenheit:
                        uom = "F"
                    else:
                        uom = "C"

                if data.startswith('TMPB'):
                    sensordata = DoFahrenheitConversion(str(data[4:].rstrip("-")))
                    db_type = 1
                    if Farenheit:
                        uom = "F"
                    else:
                        uom = "C"

                if data.startswith('HUM'):
                    sensordata = str(data[3:].rstrip("-"))
                    db_type = 5
                    measure = '2'
                    uom = "%"

                if data.startswith('BATT'):
                    sensordata = data[4:].strip('-')
                    db_type = 6
                    uom = "V"

                if data.startswith('BMP') or (bme_messages > 0 and sensordata == ''):
                    start_time = time.time()
                    if bme_messages == 0:
                        bme_data = bme_data + data[5:9]
                    else:
                        bme_data = bme_data + data[0:9]
                    bme_messages = bme_messages + 1

                    if bme_messages == 5:
                        bme280 = process_bme_reading(bme_data, devID)
                        if bme280.error != "":
                            dprint(bme280.error)
                        else:
                            if bme280.temp_rt == 1:
                                if Farenheit:
                                    uom = "F"
                                else:
                                    uom = "C"
                                ProcessMessage(DoFahrenheitConversion(round(bme280.temp, 1)), devID, 1, uom)
                            if bme280.hum_rt == 1:
                                measure = '2'
                                ProcessMessage(round(bme280.hum, 2), devID, 5, "%")
                            if bme280.hum_rt == 1:
                                measure = '2'
                                ProcessMessage(round(bme280.press / 100, 1), devID, 7, "P")
                        bme_messages = 0;
                        bme_data = ""
                if sensordata != "":
                    ProcessMessage(sensordata, devID, db_type, uom)
            sensordata = ""

            if rfsettings.event.is_set():
                break

            elapsed_time = time.time() - start_time
            if (elapsed_time > 5):
                start_time = time.time() - 120
                bme_messages = 0;
                bme_data = ""

    except Exception as e:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        print
        message
        print
        e
        rfsettings.event.set()
        exit()


def DoFahrenheitConversion(value):
    if Farenheit:
        value = float(value) * 1.8 + 32
        value = round(value, 2)
    return (value)


def main():
    dprint("started main")
    rfsettings.init()
    dprint("finished rfsettings.init()")
    a = Thread(target=rf2serial, args=())
    a.start()

    b = Thread(target=queue_processing, args=())
    b.start()

    while not rfsettings.event.is_set():
        try:
            sleep(1)
        except KeyboardInterrupt:
            rfsettings.event.set()
            break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(e).__name__, e.args)
        print
        message
        print
        e
        rfsettings.event.set()
    finally:
        rfsettings.event.set()
        exit()





