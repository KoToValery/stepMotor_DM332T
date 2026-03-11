#!/usr/bin/env python3
"""
Dedicated Stepper Motor Controller for DM332T Driver via PCA9685
Uses hardware PWM for precise pulse generation
"""
import json
import logging
import os
import signal
import sys
import threading
import time
import subprocess

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from smbus2 import SMBus
import paho.mqtt.client as mqtt

logger = logging.getLogger("stepper_motor")
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Kernel module check
try:
    logger.info("Loading i2c-dev module...")
    subprocess.run(["modprobe", "i2c-dev"], check=False)
except Exception as e:
    logger.warning("Failed to load i2c-dev: %s", e)

# PCA9685 registers
MODE1 = 0x00
MODE2 = 0x01
PRESCALE = 0xFE
LED0_ON_L = 0x06

MODE1_RESTART = 0x80
MODE1_SLEEP = 0x10
MODE1_AI = 0x20
MODE2_OUTDRV = 0x04

OSC_HZ = 25_000_000


class PCA9685:
    """PCA9685 PWM controller driver"""
    
    def __init__(self, bus_num: int, address: int):
        self.address = address
        self.bus = SMBus(bus_num)
        self._write8(MODE1, MODE1_AI)
        self._write8(MODE2, MODE2_OUTDRV)
        time.sleep(0.01)

    def close(self):
        try:
            self.bus.close()
        except Exception:
            pass

    def _write8(self, reg, val):
        self.bus.write_byte_data(self.address, reg, val & 0xFF)

    def _read8(self, reg):
        return self.bus.read_byte_data(self.address, reg)

    def set_pwm_freq(self, freq_hz: float):
        """Set the PWM frequency for all channels"""
        prescale = int(round(OSC_HZ / (4096.0 * float(freq_hz)) - 1.0))
        prescale = max(3, min(255, prescale))
        oldmode = self._read8(MODE1)
        sleepmode = (oldmode & 0x7F) | MODE1_SLEEP
        self._write8(MODE1, sleepmode)
        time.sleep(0.005)
        self._write8(PRESCALE, prescale)
        time.sleep(0.005)
        self._write8(MODE1, oldmode)
        time.sleep(0.005)
        self._write8(MODE1, oldmode | MODE1_RESTART | MODE1_AI)
        time.sleep(0.005)

    def set_pwm(self, channel: int, on: int, off: int):
        """Set PWM for a specific channel"""
        channel = int(channel)
        if not (0 <= channel <= 15):
            raise ValueError("channel must be 0..15")
        on = int(max(0, min(4095, on)))
        off = int(max(0, min(4095, off)))

        reg = LED0_ON_L + 4 * channel
        data = [on & 0xFF, (on >> 8) & 0xFF, off & 0xFF, (off >> 8) & 0xFF]
        self.bus.write_i2c_block_data(self.address, reg, data)

    def set_duty_cycle(self, channel: int, duty_percent: float):
        """Set duty cycle as percentage (0-100)"""
        duty_12bit = int((duty_percent / 100.0) * 4095)
        duty_12bit = max(0, min(4095, duty_12bit))
        self.set_pwm(channel, 0, duty_12bit)

    def channel_on(self, channel: int):
        """Turn channel fully on"""
        self.set_pwm(channel, 0, 4095)

    def channel_off(self, channel: int):
        """Turn channel fully off"""
        self.set_pwm(channel, 0, 0)


def load_config():
    """Load configuration from Home Assistant"""
    if HAS_REQUESTS:
        token = os.environ.get("SUPERVISOR_TOKEN")
        if token:
            try:
                resp = requests.get(
                    "http://supervisor/services/mqtt",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=5,
                )
                if resp.status_code == 200:
                    mqtt_cfg = resp.json()["data"]
                    with open("/data/options.json") as f:
                        opts = json.load(f)
                    opts.update({
                        "mqtt_host": mqtt_cfg["host"],
                        "mqtt_port": mqtt_cfg["port"],
                        "mqtt_username": mqtt_cfg["username"],
                        "mqtt_password": mqtt_cfg["password"],
                    })
                    return opts
            except Exception:
                pass

    with open("/data/options.json") as f:
        return json.load(f)


# Load configuration
config = load_config()

MQTT_HOST = config["mqtt_host"]
MQTT_PORT = int(config["mqtt_port"])
MQTT_USER = config.get("mqtt_username") or None
MQTT_PASS = config.get("mqtt_password") or None

I2C_BUS = int(config.get("i2c_bus", 1))
PCA_ADDR = int(config["pca_address"], 16)
PCA_FREQ = int(config.get("pca_frequency", 1000))

CH_PULSE = int(config.get("stepper_pulse_channel", 9))
CH_DIR = int(config.get("stepper_dir_channel", 7))
CH_ENA = int(config.get("stepper_ena_channel", 8))

MAX_SPEED_HZ = int(config.get("max_speed_hz", 1000))
DEFAULT_SPEED_HZ = int(config.get("default_speed_hz", 200))
STEPS_PER_REV = int(config.get("steps_per_revolution", 1000))
MICROSTEPS = int(config.get("microsteps", 1))

# MQTT topics
AVAIL_TOPIC = "homeassistant/stepper_motor/availability"

TOPIC_ENABLE_CMD = "homeassistant/switch/stepper_enable/set"
TOPIC_ENABLE_STATE = "homeassistant/switch/stepper_enable/state"

TOPIC_DIR_CMD = "homeassistant/select/stepper_direction/set"
TOPIC_DIR_STATE = "homeassistant/select/stepper_direction/state"

TOPIC_SPEED_CMD = "homeassistant/number/stepper_speed/set"
TOPIC_SPEED_STATE = "homeassistant/number/stepper_speed/state"

TOPIC_STEPS_CMD = "homeassistant/number/stepper_steps/set"
TOPIC_STEPS_STATE = "homeassistant/number/stepper_steps/state"

TOPIC_REVOLUTIONS_CMD = "homeassistant/number/stepper_revolutions/set"
TOPIC_REVOLUTIONS_STATE = "homeassistant/number/stepper_revolutions/state"

TOPIC_MICROSTEPS_CMD = "homeassistant/select/stepper_microsteps/set"
TOPIC_MICROSTEPS_STATE = "homeassistant/select/stepper_microsteps/state"

TOPIC_RUN_CMD = "homeassistant/button/stepper_run/press"
TOPIC_STOP_CMD = "homeassistant/button/stepper_stop/press"

# Initialize PCA9685
logger.info("Initializing PCA9685 at address %s on I2C bus %d", hex(PCA_ADDR), I2C_BUS)
pca = None
for attempt in range(1, 11):
    try:
        pca = PCA9685(I2C_BUS, PCA_ADDR)
        pca.set_pwm_freq(PCA_FREQ)
        logger.info("PCA9685 initialized with frequency %d Hz", PCA_FREQ)
        break
    except Exception as e:
        logger.warning("Attempt %d/10: Cannot initialize PCA9685 (%s)", attempt, e)
        time.sleep(2)

if pca is None:
    logger.error("Failed to initialize PCA9685 after 10 attempts")
    sys.exit(1)

# Stepper motor state
stepper_enabled = False
stepper_direction = "CW"  # CW or CCW
stepper_speed_hz = float(DEFAULT_SPEED_HZ)
stepper_steps = 0
stepper_revolutions = 0.0
stepper_microsteps = MICROSTEPS
stepper_running = False
stepper_lock = threading.Lock()
stepper_thread = None


def set_stepper_enable(enabled: bool):
    """Enable or disable stepper motor"""
    global stepper_enabled
    stepper_enabled = enabled
    if enabled:
        pca.channel_off(CH_ENA)  # DM332T: LOW = enabled
        logger.info("Stepper motor ENABLED")
    else:
        pca.channel_on(CH_ENA)  # DM332T: HIGH = disabled
        stop_stepper_motion()
        logger.info("Stepper motor DISABLED")


def set_stepper_direction(direction: str):
    """Set stepper motor direction"""
    global stepper_direction
    stepper_direction = "CCW" if direction == "CCW" else "CW"
    if stepper_direction == "CW":
        pca.channel_on(CH_DIR)  # HIGH = CW
    else:
        pca.channel_off(CH_DIR)  # LOW = CCW
    logger.info("Stepper direction set to %s", stepper_direction)


def set_stepper_speed(speed_hz: float):
    """Set stepper motor speed in Hz (steps per second)"""
    global stepper_speed_hz
    stepper_speed_hz = max(1.0, min(float(MAX_SPEED_HZ), float(speed_hz)))
    logger.info("Stepper speed set to %.1f Hz", stepper_speed_hz)


def set_stepper_microsteps(microsteps: int):
    """Set microstep resolution"""
    global stepper_microsteps
    valid_microsteps = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    if microsteps in valid_microsteps:
        stepper_microsteps = microsteps
        logger.info("Microsteps set to %d", stepper_microsteps)
    else:
        logger.warning("Invalid microsteps value: %d", microsteps)


def calculate_steps_from_revolutions(revolutions: float) -> int:
    """Calculate total steps from revolutions"""
    return int(revolutions * STEPS_PER_REV * stepper_microsteps)


def start_stepper_motion(steps: int):
    """Start stepper motor motion for specified number of steps"""
    global stepper_running, stepper_thread, stepper_steps
    
    if not stepper_enabled:
        logger.warning("Cannot start motion: stepper is disabled")
        return
    
    with stepper_lock:
        if stepper_running:
            logger.warning("Stepper already running")
            return
        
        stepper_steps = int(steps)
        stepper_running = True
        stepper_thread = threading.Thread(target=stepper_motion_worker, daemon=True)
        stepper_thread.start()
        logger.info("Started stepper motion: %d steps at %.1f Hz (microsteps: %d)", 
                   steps, stepper_speed_hz, stepper_microsteps)


def stop_stepper_motion():
    """Stop stepper motor motion"""
    global stepper_running
    
    with stepper_lock:
        if stepper_running:
            stepper_running = False
            logger.info("Stopping stepper motion")
    
    # Stop PWM on pulse channel
    pca.channel_off(CH_PULSE)


def stepper_motion_worker():
    """Worker thread for stepper motor motion using hardware PWM"""
    global stepper_running
    
    try:
        steps_remaining = stepper_steps
        speed = stepper_speed_hz
        
        # Calculate PWM duty cycle (50% for square wave)
        duty_cycle = 50.0
        
        # Set PWM frequency to match step speed
        # Note: This changes the global PCA9685 frequency
        pca.set_pwm_freq(speed)
        
        # Enable PWM on pulse channel
        pca.set_duty_cycle(CH_PULSE, duty_cycle)
        
        # Calculate total time for motion
        if steps_remaining > 0:
            total_time = steps_remaining / speed
            time.sleep(total_time)
        else:
            # Continuous motion
            while stepper_running:
                time.sleep(0.1)
        
    except Exception as e:
        logger.error("Error in stepper motion: %s", e)
    finally:
        # Stop PWM
        pca.channel_off(CH_PULSE)
        # Restore default PCA frequency
        pca.set_pwm_freq(PCA_FREQ)
        
        with stepper_lock:
            stepper_running = False
        
        logger.info("Stepper motion completed")


# Initialize channels
pca.channel_off(CH_PULSE)
pca.channel_on(CH_ENA)  # Disabled by default
pca.channel_on(CH_DIR)  # CW by default

# MQTT client setup
try:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
except (AttributeError, TypeError):
    client = mqtt.Client()

if MQTT_USER and MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

device_info = {
    "identifiers": ["stepper_motor_dm332t"],
    "name": "Stepper Motor DM332T",
    "model": "DM332T + 17HS15-1684S-PG5",
    "manufacturer": "biCOMM Design",
    "sw_version": "1.0.0",
}


def publish_discovery():
    """Publish MQTT discovery messages"""
    discoveries = [
        ("switch", "stepper_enable", {
            "name": "Stepper Enable",
            "unique_id": "stepper_enable",
            "command_topic": TOPIC_ENABLE_CMD,
            "state_topic": TOPIC_ENABLE_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "icon": "mdi:power",
            "device": device_info,
        }),
        ("select", "stepper_direction", {
            "name": "Stepper Direction",
            "unique_id": "stepper_direction",
            "command_topic": TOPIC_DIR_CMD,
            "state_topic": TOPIC_DIR_STATE,
            "availability_topic": AVAIL_TOPIC,
            "options": ["CW", "CCW"],
            "icon": "mdi:rotate-right",
            "device": device_info,
        }),
        ("number", "stepper_speed", {
            "name": "Stepper Speed",
            "unique_id": "stepper_speed",
            "command_topic": TOPIC_SPEED_CMD,
            "state_topic": TOPIC_SPEED_STATE,
            "availability_topic": AVAIL_TOPIC,
            "min": 1,
            "max": MAX_SPEED_HZ,
            "step": 1,
            "unit_of_measurement": "Hz",
            "mode": "slider",
            "icon": "mdi:speedometer",
            "device": device_info,
        }),
        ("select", "stepper_microsteps", {
            "name": "Microsteps",
            "unique_id": "stepper_microsteps",
            "command_topic": TOPIC_MICROSTEPS_CMD,
            "state_topic": TOPIC_MICROSTEPS_STATE,
            "availability_topic": AVAIL_TOPIC,
            "options": ["1", "2", "4", "8", "16", "32", "64", "128", "256"],
            "icon": "mdi:stairs",
            "device": device_info,
        }),
        ("number", "stepper_steps", {
            "name": "Stepper Steps",
            "unique_id": "stepper_steps",
            "command_topic": TOPIC_STEPS_CMD,
            "state_topic": TOPIC_STEPS_STATE,
            "availability_topic": AVAIL_TOPIC,
            "min": 0,
            "max": 100000,
            "step": 1,
            "unit_of_measurement": "steps",
            "mode": "box",
            "icon": "mdi:counter",
            "device": device_info,
        }),
        ("number", "stepper_revolutions", {
            "name": "Revolutions",
            "unique_id": "stepper_revolutions",
            "command_topic": TOPIC_REVOLUTIONS_CMD,
            "state_topic": TOPIC_REVOLUTIONS_STATE,
            "availability_topic": AVAIL_TOPIC,
            "min": 0,
            "max": 100,
            "step": 0.1,
            "unit_of_measurement": "rev",
            "mode": "box",
            "icon": "mdi:rotate-360",
            "device": device_info,
        }),
        ("button", "stepper_run", {
            "name": "Run Stepper",
            "unique_id": "stepper_run",
            "command_topic": TOPIC_RUN_CMD,
            "availability_topic": AVAIL_TOPIC,
            "icon": "mdi:play",
            "device": device_info,
        }),
        ("button", "stepper_stop", {
            "name": "Stop Stepper",
            "unique_id": "stepper_stop",
            "command_topic": TOPIC_STOP_CMD,
            "availability_topic": AVAIL_TOPIC,
            "icon": "mdi:stop",
            "device": device_info,
        }),
    ]

    for component, unique_id, payload in discoveries:
        topic = f"homeassistant/{component}/{unique_id}/config"
        client.publish(topic, json.dumps(payload), retain=True)

    client.publish(AVAIL_TOPIC, "online", retain=True)
    
    # Publish initial states
    client.publish(TOPIC_ENABLE_STATE, "OFF", retain=True)
    client.publish(TOPIC_DIR_STATE, stepper_direction, retain=True)
    client.publish(TOPIC_SPEED_STATE, str(int(stepper_speed_hz)), retain=True)
    client.publish(TOPIC_MICROSTEPS_STATE, str(stepper_microsteps), retain=True)
    client.publish(TOPIC_STEPS_STATE, str(stepper_steps), retain=True)
    client.publish(TOPIC_REVOLUTIONS_STATE, str(stepper_revolutions), retain=True)

    logger.info("MQTT discovery published")


def on_connect(client, userdata, flags, reason_code, properties=None):
    """MQTT connection callback"""
    rc = reason_code.value if hasattr(reason_code, "value") else reason_code
    if rc != 0:
        logger.error("MQTT connection failed with code %s", rc)
        return

    client.subscribe(TOPIC_ENABLE_CMD)
    client.subscribe(TOPIC_DIR_CMD)
    client.subscribe(TOPIC_SPEED_CMD)
    client.subscribe(TOPIC_MICROSTEPS_CMD)
    client.subscribe(TOPIC_STEPS_CMD)
    client.subscribe(TOPIC_REVOLUTIONS_CMD)
    client.subscribe(TOPIC_RUN_CMD)
    client.subscribe(TOPIC_STOP_CMD)
    
    publish_discovery()
    logger.info("Connected to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)


def on_message(client, userdata, msg):
    """MQTT message callback"""
    global stepper_revolutions
    topic = msg.topic
    payload = msg.payload.decode("utf-8").strip()

    try:
        if topic == TOPIC_ENABLE_CMD:
            enabled = (payload == "ON")
            set_stepper_enable(enabled)
            client.publish(TOPIC_ENABLE_STATE, "ON" if enabled else "OFF", retain=True)

        elif topic == TOPIC_DIR_CMD:
            set_stepper_direction(payload)
            client.publish(TOPIC_DIR_STATE, stepper_direction, retain=True)

        elif topic == TOPIC_SPEED_CMD:
            speed = float(payload)
            set_stepper_speed(speed)
            client.publish(TOPIC_SPEED_STATE, str(int(stepper_speed_hz)), retain=True)

        elif topic == TOPIC_MICROSTEPS_CMD:
            microsteps = int(payload)
            set_stepper_microsteps(microsteps)
            client.publish(TOPIC_MICROSTEPS_STATE, str(stepper_microsteps), retain=True)

        elif topic == TOPIC_STEPS_CMD:
            steps = int(payload)
            client.publish(TOPIC_STEPS_STATE, str(steps), retain=True)

        elif topic == TOPIC_REVOLUTIONS_CMD:
            revolutions = float(payload)
            stepper_revolutions = revolutions
            # Calculate and update steps
            steps = calculate_steps_from_revolutions(revolutions)
            client.publish(TOPIC_REVOLUTIONS_STATE, str(revolutions), retain=True)
            client.publish(TOPIC_STEPS_STATE, str(steps), retain=True)

        elif topic == TOPIC_RUN_CMD:
            # Get current steps value
            steps = stepper_steps if stepper_steps > 0 else calculate_steps_from_revolutions(stepper_revolutions)
            start_stepper_motion(steps)

        elif topic == TOPIC_STOP_CMD:
            stop_stepper_motion()

    except Exception as e:
        logger.exception("Error processing topic=%s payload=%s: %s", topic, payload, e)


client.on_connect = on_connect
client.on_message = on_message


def safe_shutdown(signum=None, frame=None):
    """Graceful shutdown"""
    logger.info("Shutting down...")
    try:
        stop_stepper_motion()
        set_stepper_enable(False)
        
        pca.channel_off(CH_PULSE)
        pca.channel_on(CH_ENA)
        pca.channel_off(CH_DIR)

        client.publish(AVAIL_TOPIC, "offline", retain=True)
        client.loop_stop()
        client.disconnect()
        pca.close()

    except Exception as e:
        logger.exception("Shutdown error: %s", e)

    sys.exit(0 if signum is not None else 1)


signal.signal(signal.SIGTERM, safe_shutdown)
signal.signal(signal.SIGINT, safe_shutdown)

# Connect to MQTT
logger.info("Connecting to MQTT %s:%s...", MQTT_HOST, MQTT_PORT)
for attempt in range(1, 11):
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()
        time.sleep(2)
        if client.is_connected():
            break
        raise RuntimeError("Connection timeout")
    except Exception as e:
        if attempt == 10:
            logger.error("MQTT connection failed after 10 attempts: %s", e)
            safe_shutdown()
        time.sleep(1.5 ** (attempt - 1))

logger.info("Stepper Motor Controller running")

# Main loop
while True:
    time.sleep(5)
    if not client.is_connected():
        try:
            client.reconnect()
            client.publish(AVAIL_TOPIC, "online", retain=True)
        except Exception as e:
            logger.exception("MQTT reconnect failed: %s", e)
