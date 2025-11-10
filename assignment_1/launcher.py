import time
import machine
from umqtt.simple2 import MQTTClient
from dht import DHT11
import uasyncio as asyncio
from machine import Pin
import os
import network
import sys


LOG_PATH = "error.log"
CFG_PATH = "config.txt"
AP_CONNECTION_TIMEOUT_SEC = 20


class Utils: 
    @classmethod
    def get_unix_timestamp(cls) -> int:
        return int(time.time())
    
    @classmethod
    def is_file(cls, file_path: str) -> bool:
        """Check if a file exists."""
        try: 
            os.stat(file_path)
            return True
        except: 
            return False

    @classmethod
    def remove_file(cls, file_path: str):
        """Remove a file. Raises OSError if fails."""
        try: 
            os.remove(file_path)
        except OSError as e: 
            raise OSError(f"Exception occurred trying to remove file: '{file_path}'") from e

    @classmethod
    def write_to_file(cls, file_path: str, message: str):
        """Append message to file."""
        with open(file_path, "a") as f: 
            f.write(message)

    @classmethod
    def connect_wifi(cls, ssid: str, password: str, timeout_sec: int = 20):
        """Connect to WiFi with timeout and verify IP assignment."""
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)

        # Ensure clean state
        if wlan.isconnected():
            wlan.disconnect()
            time.sleep(1)

        wlan.connect(ssid, password)

        start = time.time()
        while True:
            if wlan.isconnected():
                ip_info = wlan.ifconfig()
                if ip_info[0] != "0.0.0.0":  # Check if IP assigned
                    print("Connected! IP:", ip_info[0])
                    return wlan
            if time.time() - start > timeout_sec:
                raise OSError("Wi-Fi connection failed (timeout).")
            time.sleep(0.5)



class Logger: 
    def __init__(self, class_name: str, file_path: str):
        self.file_path = file_path
        self.class_name = class_name

        # TASK 1: Create empty error.log file, delete if exists
        if Utils.is_file(file_path):
            Utils.remove_file(file_path)

        self._create_error_log_file(file_path)

    def _create_error_log_file(self, file_path: str): 
        """TASK 1: Creates error.log file with timestamp."""
        timestamp = Utils.get_unix_timestamp()

        try:
            with open(file_path, 'w') as f:
                f.write(f"[{timestamp}] Log file '{file_path}' created.\n")
        except Exception as e: 
            # TASK 1: Raise OSError if log file cannot be created
            raise OSError(f"Exception occurred trying to create log file: '{file_path}'") from e

    def _write(self, level, message):
        """TASK 1: Write log entries with Unix timestamp."""
        timestamp = int(time.time())
        line = f"[{timestamp}] {self.class_name} {level}: {message}\n"

        try: 
            print(line.strip())
            Utils.write_to_file(self.file_path, line)  
        except OSError as e:
            raise OSError(f"Exception occurred writing to log file: '{self.file_path}'")
        
    def _print(self, level, message):
        """Print without writing to file."""
        timestamp = int(time.time())
        line = f"[{timestamp}] {self.class_name} {level}: {message}"
        print(line)

    def info(self, message: str): 
        self._print("INFO", message) 

    def error(self, message: str): 
        self._write("ERROR", message) 


class DHTMQTTSubscriber:
    def __init__(self, mqtt_client, topic_name: str, logger: Logger):
        self.topic_name = topic_name
        self._logger = logger
        self._mqtt_client = mqtt_client
        self._mqtt_client.set_callback(self._on_msg_rcv_cb)
        # TASK 6.ii: Subscribe to the topic
        self._mqtt_client.subscribe(topic_name)
        # TASK 6.vi: Initialize onboard LED
        self._led_pin = Pin("LED", Pin.OUT)
        self._worker_task = None 
        self._active = False
        self._subscription_storage_file = "subscriber.csv"

        self._logger.info(f"Subscriber initialized for topic '{self.topic_name}'")

    def is_active(self) -> bool:
        return self._active
    
    def _create_subscriber_csv(self):
        """TASK 6.iv: Create subscriber.csv if it does not exist."""
        if not Utils.is_file(self._subscription_storage_file):
            self._logger.info(f"Creating subscription file: {self._subscription_storage_file}")
            with open(self._subscription_storage_file, "w") as f: 
                # TASK 6.iv: First line should contain "UNIX_TS, C, %"
                f.write("UNIX_TS, C, %\n")

    def activate(self, timeout_sec: float = 1.0):
        """Start the MQTT subscription worker."""
        if not self._active:
            self._active = True
            # TASK 6.iv: Create CSV file only if it does not exist
            self._create_subscriber_csv()
            self._worker_task = asyncio.create_task(self._subscription_worker(timeout_sec))
            self._logger.info(f"Subscriber activated on topic: {self.topic_name}")

    def deactivate(self):
        """TASK 9: Stop the MQTT subscription worker."""
        if self._active:
            self._active = False
            if self._worker_task:
                self._worker_task.cancel()
                self._worker_task = None
            self._logger.info(f"Subscriber deactivated on topic: {self.topic_name}")

    async def _subscription_worker(self, timeout_sec: float = 1.0):
        """Background worker that checks for messages."""
        while self._active:
            try:
                self._mqtt_client.check_msg()
            except Exception as e:
                self._logger.error(f"Exception in subscription worker: {str(e)}")
            await asyncio.sleep(timeout_sec)

    def _on_msg_rcv_cb(self, topic: bytes, msg: bytes, retained: bool = False, dup: bool = False):
        """TASK 6.v: Parse message and store in CSV."""
        topic_str = topic.decode("utf-8")
        msg_str = msg.decode("utf-8")
        self._logger.info(f"Message received on {topic_str}: {msg_str}")

        try:
            # TASK 6.v: Parse Unix timestamp, temperature, and humidity
            parts = msg_str.split(",")
            unix_ts = parts[0].strip()
            temp = parts[1].strip()
            # Remove "C" if present
            temp = temp.replace("C", "").strip()
            humidity = parts[2].strip()
            # Remove "%" if present
            humidity = humidity.replace("%", "").strip()
            
            # TASK 6.v: Store values in CSV file, append if exists
            with open(self._subscription_storage_file, "a") as f:
                f.write(f"{unix_ts}, {temp}, {humidity}\n")
            
            # TASK 6.vi: Turn on LED for 1 second when message received
            asyncio.create_task(self._blink_led())
        except Exception as e:
            self._logger.error(f"Exception in message callback: {str(e)}")

    async def _blink_led(self, period: float = 1.0):
        """TASK 6.vi: Turn on LED for 1 second then turn off."""
        self._led_pin.value(1)
        await asyncio.sleep(period)
        self._led_pin.value(0)


class DHTMQTTPublisher: 
    def __init__(self, mqtt_client, topic_name: str, logger: Logger, digit_sum: int):
        self._mqtt_client = mqtt_client
        self._logger = logger
        self.topic_name = topic_name
        self.digit_sum = digit_sum
        # DHT11 connected to GPIO22
        self._dht_pin = Pin(22, Pin.IN)
        self._dht_sensor = DHT11(self._dht_pin)
        self._worker_task = None
        self._active = False

        self._logger.info(f"Publisher initialized for topic '{self.topic_name}'")

    def is_active(self) -> bool: 
        return self._active
    
    def activate(self, timeout_sec: float = 15.0):
        """Start the MQTT publisher worker."""
        if not self._active:
            self._active = True
            # TASK 8.v: Publish message every 15 seconds
            self._worker_task = asyncio.create_task(self._publisher_worker(timeout_sec))
            self._logger.info(f"Publisher activated on topic: {self.topic_name}")

    def deactivate(self):
        """TASK 9: Stop the MQTT publisher worker."""
        if self._active:
            self._active = False
            if self._worker_task:
                self._worker_task.cancel()
                self._worker_task = None
            self._logger.info(f"Publisher deactivated on topic: {self.topic_name}")

    def _read_dht_sensor(self, max_attempts: int = 10):
        """TASK 8.iii: Read sensor values, retry up to 10 times."""
        for attempt in range(max_attempts):
            try:
                self._dht_sensor.measure()
                temp = self._dht_sensor.temperature()
                hum = self._dht_sensor.humidity()
                return temp, hum
            except Exception as e:
                if attempt == max_attempts - 1:
                    # TASK 8.iii: Raise OSError if not available after 10 tries
                    raise OSError(f"Sensor values not available after {max_attempts} attempts")
                time.sleep(0.5)

    def _publish_payload(self, payload): 
        self._mqtt_client.publish(self.topic_name, payload)

    async def _publisher_worker(self, timeout_sec: float = 15.0):
        """TASK 8: Publish DHT sensor data."""
        while self._active: 
            try:
                # TASK 8.iii: Read temperature and humidity from DHT11
                temp, hum = self._read_dht_sensor(max_attempts=10)
                unix_ts = Utils.get_unix_timestamp()

                # TASK 8.iii: Create message in required format
                # Format: UNIX_TS, TEMPERATURE_VALUE, C, HUMIDITY_VALUE, %, DIGIT_SUM
                payload = f"{unix_ts}, {temp}, C, {hum}, %, {self.digit_sum}"
                self._publish_payload(payload)
                
                self._logger.info(f"Published payload: {payload}")
            except OSError as e:
                self._logger.error(str(e))
                raise
            except Exception as e:
                self._logger.error(f"Exception while publishing: {str(e)}")
            
            # TASK 8.v: Publish every 15 seconds
            await asyncio.sleep(timeout_sec)


async def main(**kwargs):
    # TASK 1: Initialize logger which creates error.log
    logger = Logger("Launcher", LOG_PATH)
    logger.info("Initializing Launcher...")

    # Get configuration path and student ID from kwargs
    cfg_path = str(kwargs.get("cfg_path", "config.txt"))
    student_id = int(kwargs.get("student_id", 0))

    # TASK 2: Read config.txt configuration file
    def extract_launcher_cfg():
        cfg = {}

        # TASK 2: Check if config file exists, raise SystemExit if not
        if not Utils.is_file(cfg_path):
            error_msg = f"Configuration file '{cfg_path}' does not exist."
            logger.error(error_msg)
            raise SystemExit(error_msg)

        # TASK 2: Read configuration file
        try:
            with open(cfg_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '=' in line:
                        key, value = line.split('=', 1)
                        cfg[key.strip()] = value.strip()
        except Exception as e:
            error_msg = f"Cannot read configuration file '{cfg_path}': {str(e)}"
            logger.error(error_msg)
            raise SystemExit(error_msg)

        # Validate required keys
        required_keys = [
            'ACCESS_POINT_NAME',
            'ACCESS_POINT_PASSWORD',
            'MQTT_BROKER_HOSTNAME',
            'MQTT_BROKER_PORT'
        ]
        for k in required_keys:
            if k not in cfg:
                error_msg = f"Missing required config key: {k}"
                logger.error(error_msg)
                raise SystemExit(error_msg)

        return (
            str(cfg['ACCESS_POINT_NAME']),
            str(cfg['ACCESS_POINT_PASSWORD']),
            str(cfg['MQTT_BROKER_HOSTNAME']),
            int(cfg['MQTT_BROKER_PORT'])
        )
    
    # TASK 2: Extract configuration
    access_point_name, access_point_password, mqtt_broker_hostname, mqtt_broker_port = extract_launcher_cfg()
    
    # TASK 3: Calculate digit sum of student ID
    DIGIT_SUM = sum(int(d) for d in str(student_id))
    logger.info(f"Student ID: {student_id}, Digit Sum: {DIGIT_SUM}")
    
    # TASK 4: Create UNIX_TS variable with Unix timestamp
    UNIX_TS = Utils.get_unix_timestamp()
    logger.info(f"Unix Timestamp: {UNIX_TS}")

    # TASK 5: Establish WiFi connection within 20 seconds
    logger.info(f"Attempting WiFi connection to: '{access_point_name}'")
    try:
        wlan = Utils.connect_wifi(access_point_name, access_point_password, AP_CONNECTION_TIMEOUT_SEC)
        # TASK 6.ii: Get MAC address without colons for topic
        wlan_mac = ''.join(f'{b:02X}' for b in wlan.config('mac'))
        logger.info(f"WiFi connected - MAC: {wlan_mac}")
    except OSError as e:
        # TASK 5: Raise OSError and terminate if connection fails
        logger.error(str(e))
        raise

    # Get unique board ID for client IDs
    BOARD_ID = ''.join(f'{b:02X}' for b in machine.unique_id())
    
    # TASK 6.ii: Create topic using MAC address and UNIX_TS
    mqtt_topic_name = f"/sensor/{wlan_mac}/{UNIX_TS}/"
    
    # TASK 6.iii: Create subscriber client ID
    subscriber_client_id = f"{BOARD_ID}-{UNIX_TS}-sub"

    # TASK 6.i: Establish connection with MQTT broker for subscriber
    logger.info(f"Connecting MQTT Subscriber:")
    logger.info(f"  Broker: {mqtt_broker_hostname}:{mqtt_broker_port}")
    logger.info(f"  Client ID: {subscriber_client_id}")
    logger.info(f"  Topic: {mqtt_topic_name}")

    try:
        subscriber_mqtt_client = MQTTClient(
            client_id=subscriber_client_id,
            server=mqtt_broker_hostname,
            port=mqtt_broker_port,
        )
        subscriber_mqtt_client.connect()
        logger.info("MQTT Subscriber connected successfully!")
    except Exception as e:
        # TASK 6.i: Raise RuntimeError if connection cannot be established
        error_msg = f"Cannot establish MQTT subscriber connection: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # TASK 6: Initialize and activate MQTT subscriber
    dht_mqtt_subscriber = DHTMQTTSubscriber(subscriber_mqtt_client, mqtt_topic_name, logger)
    dht_mqtt_subscriber.activate(timeout_sec=1.0)

    # TASK 7: Wait 15 seconds
    logger.info("Waiting 15 seconds before starting publisher...")
    await asyncio.sleep(15.0)

    # TASK 8.iv: Create publisher client ID (different from subscriber)
    publisher_client_id = f"{BOARD_ID}-{UNIX_TS}-pub"

    # TASK 8.i: Establish connection with MQTT broker for publisher
    logger.info(f"Connecting MQTT Publisher:")
    logger.info(f"  Broker: {mqtt_broker_hostname}:{mqtt_broker_port}")
    logger.info(f"  Client ID: {publisher_client_id}")
    logger.info(f"  Topic: {mqtt_topic_name}")

    try:
        publisher_mqtt_client = MQTTClient(
            client_id=publisher_client_id,
            server=mqtt_broker_hostname,
            port=mqtt_broker_port,
        )
        publisher_mqtt_client.connect()
        logger.info("MQTT Publisher connected successfully!")
    except Exception as e:
        # TASK 8.i: Raise RuntimeError if connection cannot be established
        error_msg = f"Cannot establish MQTT publisher connection: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # TASK 8: Initialize and activate MQTT publisher
    dht_mqtt_publisher = DHTMQTTPublisher(publisher_mqtt_client, mqtt_topic_name, logger, DIGIT_SUM)
    # TASK 8.v: Publish message every 15 seconds
    dht_mqtt_publisher.activate(timeout_sec=15.0)

    # TASK 9: Wait 5 minutes (300 seconds)
    logger.info("Running for 5 minutes...")
    await asyncio.sleep(300)
    
    # TASK 9: Disconnect subscriber and publisher after 5 minutes
    logger.info("Deactivating publisher and subscriber...")
    dht_mqtt_publisher.deactivate()
    dht_mqtt_subscriber.deactivate()
    
    subscriber_mqtt_client.disconnect()
    publisher_mqtt_client.disconnect()
    logger.info("MQTT clients disconnected. Program complete.")


if __name__ == '__main__':
    try:
        asyncio.run(main(
            cfg_path="config.txt",
            student_id=201954749  # Replace with your actual student ID
        ))
    except KeyboardInterrupt:
        print("Program stopped by user")
    except Exception as e:
        print(f"Program terminated with error: {str(e)}")