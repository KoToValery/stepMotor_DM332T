#!/usr/bin/env python3
"""
PCA9685 PWM Controller for Home Assistant
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

logger = logging.getLogger("pca9685_pwm")
_handler = logging.StreamHandler(stream=sys.stdout)
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# --- Kernel Module Check ---
try:
    logger.info("Checking i2c-dev module...")
    subprocess.run(["modprobe", "i2c-dev"], check=False)
except Exception as e:
    logger.warning("Failed to run modprobe i2c-dev: %s", e)

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
        channel = int(channel)
        if not (0 <= channel <= 15):
            raise ValueError("channel must be 0..15")
        on = int(max(0, min(4095, on)))
        off = int(max(0, min(4095, off)))

        reg = LED0_ON_L + 4 * channel
        data = [on & 0xFF, (on >> 8) & 0xFF, off & 0xFF, (off >> 8) & 0xFF]
        self.bus.write_i2c_block_data(self.address, reg, data)

    def set_duty_12bit(self, channel: int, duty: int):
        duty = int(max(0, min(4095, duty)))
        self.set_pwm(channel, 0, duty)


CH_PWM1 = 0
CH_HEATER_1 = 1
CH_HEATER_2 = 2
CH_HEATER_3 = 3
CH_HEATER_4 = 4
CH_FAN_1 = 5
CH_FAN_2 = 6
CH_STEPPER_DIR = 7
CH_STEPPER_ENA = 8
CH_PU = 9
CH_SYS_LED = 15


def load_config():
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
                    opts.update(
                        {
                            "mqtt_host": mqtt_cfg["host"],
                            "mqtt_port": mqtt_cfg["port"],
                            "mqtt_username": mqtt_cfg["username"],
                            "mqtt_password": mqtt_cfg["password"],
                        }
                    )
                    return opts
            except Exception:
                pass

    with open("/data/options.json") as f:
        return json.load(f)


config = load_config()

MQTT_HOST = config["mqtt_host"]
MQTT_PORT = int(config["mqtt_port"])
MQTT_USER = config.get("mqtt_username") or None
MQTT_PASS = config.get("mqtt_password") or None

I2C_BUS = int(config.get("i2c_bus", 1))
PCA_ADDR = int(config["pca_address"], 16)
PCA_FREQ = int(config.get("pca_frequency", 1000))

DEFAULT_DUTY_CYCLE = int(config.get("default_duty_cycle", 30))
DEFAULT_DUTY_CYCLE = max(0, min(100, DEFAULT_DUTY_CYCLE))

PU_DEFAULT_HZ = float(config.get("pu_default_hz", 10.0))
PU_DEFAULT_HZ = max(0.0, PU_DEFAULT_HZ)

AVAIL_TOPIC = "homeassistant/pca9685_pwm/availability"


def _topic(component: str, unique_id: str, suffix: str) -> str:
    return f"homeassistant/{component}/{unique_id}/{suffix}"


TOPIC_PWM1_ENABLE_CMD = _topic("switch", "pca_pwm1_enable", "set")
TOPIC_PWM1_ENABLE_STATE = _topic("switch", "pca_pwm1_enable", "state")

TOPIC_PWM1_DUTY_CMD = _topic("number", "pca_pwm1_duty", "set")
TOPIC_PWM1_DUTY_STATE = _topic("number", "pca_pwm1_duty", "state")

TOPIC_HEATER_1_CMD = _topic("switch", "pca_heater_1", "set")
TOPIC_HEATER_1_STATE = _topic("switch", "pca_heater_1", "state")
TOPIC_HEATER_2_CMD = _topic("switch", "pca_heater_2", "set")
TOPIC_HEATER_2_STATE = _topic("switch", "pca_heater_2", "state")
TOPIC_HEATER_3_CMD = _topic("switch", "pca_heater_3", "set")
TOPIC_HEATER_3_STATE = _topic("switch", "pca_heater_3", "state")
TOPIC_HEATER_4_CMD = _topic("switch", "pca_heater_4", "set")
TOPIC_HEATER_4_STATE = _topic("switch", "pca_heater_4", "state")

TOPIC_FAN_1_CMD = _topic("switch", "pca_fan_1", "set")
TOPIC_FAN_1_STATE = _topic("switch", "pca_fan_1", "state")
TOPIC_FAN_2_CMD = _topic("switch", "pca_fan_2", "set")
TOPIC_FAN_2_STATE = _topic("switch", "pca_fan_2", "state")

TOPIC_STEPPER_DIR_CMD = _topic("select", "pca_stepper_dir", "set")
TOPIC_STEPPER_DIR_STATE = _topic("select", "pca_stepper_dir", "state")

TOPIC_STEPPER_ENA_CMD = _topic("switch", "pca_stepper_ena", "set")
TOPIC_STEPPER_ENA_STATE = _topic("switch", "pca_stepper_ena", "state")

TOPIC_PU_ENABLE_CMD = _topic("switch", "pca_pu_enable", "set")
TOPIC_PU_ENABLE_STATE = _topic("switch", "pca_pu_enable", "state")

TOPIC_PU_FREQ_CMD = _topic("number", "pca_pu_freq_hz", "set")
TOPIC_PU_FREQ_STATE = _topic("number", "pca_pu_freq_hz", "state")


def validate_fixed_mapping():
    channels = [
        CH_PWM1,
        CH_HEATER_1,
        CH_HEATER_2,
        CH_HEATER_3,
        CH_HEATER_4,
        CH_FAN_1,
        CH_FAN_2,
        CH_STEPPER_DIR,
        CH_STEPPER_ENA,
        CH_PU,
        CH_SYS_LED,
    ]
    for ch in channels:
        if not (0 <= ch <= 15):
            raise ValueError(f"Invalid channel {ch}; expected 0..15")
    if len(set(channels)) != len(channels):
        raise ValueError("Fixed channel mapping contains duplicates")


validate_fixed_mapping()
logger.info("Fixed channels: PWM1=0, Heaters=1-4, Fans=5-6, DIR=7, ENA=8, PU=9, SYS_LED=15")


def channel_on(channel: int):
    pca.set_duty_12bit(channel, 4095)


def channel_off(channel: int):
    pca.set_duty_12bit(channel, 0)


logger.info("Opening I2C bus %s, PCA9685 addr=%s", I2C_BUS, hex(PCA_ADDR))
pca = None
for attempt in range(1, 11):
    try:
        pca = PCA9685(I2C_BUS, PCA_ADDR)
        pca.set_pwm_freq(PCA_FREQ)
        logger.info("PCA9685 global PWM frequency set to %s Hz", PCA_FREQ)
        break
    except Exception as e:
        logger.warning("Attempt %d/10: Cannot initialize PCA9685 (%s). Retrying in 2s...", attempt, e)
        time.sleep(2)

if pca is None:
    logger.error("Fatal: Failed to initialize PCA9685 after 10 attempts.")
    sys.exit(1)


pwm1_enabled = False
pwm1_value = 0.0
pwm1_lock = threading.Lock()

heater_1 = False
heater_2 = False
heater_3 = False
heater_4 = False
fan_1 = False
fan_2 = False

stepper_dir = "CW"
stepper_ena = False

pu_enabled = False
pu_freq_hz = float(PU_DEFAULT_HZ)
pu_lock = threading.Lock()
pu_thread = None
pu_running = False

sys_led_thread = None
sys_led_running = False
sys_led_lock = threading.Lock()


def update_pwm1_output_locked():
    global pwm1_enabled, pwm1_value

    if not pwm1_enabled:
        pca.set_duty_12bit(CH_PWM1, 4095)
        return

    visual = float(pwm1_value)
    if visual == 0.0:
        pwm_percent = 100.0
    elif 0.0 < visual <= 10.0:
        pwm_percent = 90.0
    else:
        pwm_percent = 100.0 - visual

    duty = int((pwm_percent / 100.0) * 4095)
    duty = max(0, min(4095, duty))
    pca.set_duty_12bit(CH_PWM1, duty)


def apply_switch(channel: int, state: bool):
    if state:
        channel_on(channel)
    else:
        channel_off(channel)


def stepper_apply_dir(value: str):
    global stepper_dir
    stepper_dir = "CCW" if value == "CCW" else "CW"
    apply_switch(CH_STEPPER_DIR, stepper_dir == "CW")


def stepper_apply_ena(state: bool):
    global stepper_ena
    stepper_ena = bool(state)
    apply_switch(CH_STEPPER_ENA, stepper_ena)


def pu_worker():
    """
    ПОПРАВЕНА ВЕРСИЯ: Използва хардуерен PWM на PCA9685 вместо софтуерно превключване
    Това решава проблема с неточното генериране на импулси за стъпковия мотор
    """
    global pu_running
    last_enabled = None
    last_freq = None
    original_pca_freq = PCA_FREQ  # Запазваме оригиналната честота

    while pu_running:
        with pu_lock:
            enabled = bool(pu_enabled)
            freq = float(pu_freq_hz)

        if (not enabled) or freq <= 0.0:
            # Изключваме PWM
            if last_enabled is not False:
                pca.set_duty_12bit(CH_PU, 0)
                # Връщаме оригиналната честота на PCA9685
                try:
                    pca.set_pwm_freq(original_pca_freq)
                    logger.info("PU: Restored PCA9685 frequency to %d Hz", original_pca_freq)
                except Exception as e:
                    logger.warning("Failed to restore PCA frequency: %s", e)
                last_enabled = False
                last_freq = None
            time.sleep(0.1)
            continue

        # Ако честотата се е променила или току-що сме включили
        if last_freq != freq or last_enabled is not True:
            try:
                # ВАЖНО: Задаваме PWM честотата на PCA9685 да съвпада с желаната честота
                # Това променя честотата на ВСИЧКИ канали на PCA9685!
                pca.set_pwm_freq(freq)
                # Задаваме 50% duty cycle за квадратна вълна (2048 = 50% от 4095)
                pca.set_duty_12bit(CH_PU, 2048)
                last_freq = freq
                last_enabled = True
                logger.info("PU: Hardware PWM set to %.1f Hz, 50%% duty cycle", freq)
            except Exception as e:
                logger.error("Failed to set PU PWM: %s", e)
        
        time.sleep(0.1)  # Проверяваме за промени всеки 100ms

    # Изключваме PWM при спиране
    pca.set_duty_12bit(CH_PU, 0)
    try:
        pca.set_pwm_freq(original_pca_freq)
        logger.info("PU: Stopped, restored frequency to %d Hz", original_pca_freq)
    except Exception as e:
        logger.warning("Failed to restore PCA frequency on stop: %s", e)


def pu_start():
    global pu_thread, pu_running
    with pu_lock:
        if pu_thread and pu_thread.is_alive():
            return
        pu_running = True
        pu_thread = threading.Thread(target=pu_worker, daemon=True)
        pu_thread.start()
    logger.info("CH9 PU worker started")


def pu_stop():
    global pu_thread, pu_running
    with pu_lock:
        pu_running = False
    if pu_thread and pu_thread.is_alive():
        pu_thread.join(timeout=2)
    pu_thread = None
    channel_off(CH_PU)


def sys_led_worker():
    global sys_led_running
    level = False
    last_written = None

    while sys_led_running:
        level = not level
        if level != last_written:
            if level:
                channel_on(CH_SYS_LED)
            else:
                channel_off(CH_SYS_LED)
            last_written = level
        time.sleep(1.0)

    channel_off(CH_SYS_LED)


def sys_led_start():
    global sys_led_thread, sys_led_running
    with sys_led_lock:
        if sys_led_thread and sys_led_thread.is_alive():
            return
        sys_led_running = True
        sys_led_thread = threading.Thread(target=sys_led_worker, daemon=True)
        sys_led_thread.start()
    logger.info("CH15 system LED blinking started")


def sys_led_stop():
    global sys_led_thread, sys_led_running
    with sys_led_lock:
        sys_led_running = False
    if sys_led_thread and sys_led_thread.is_alive():
        sys_led_thread.join(timeout=2)
    sys_led_thread = None
    channel_off(CH_SYS_LED)


try:
    pca.set_duty_12bit(CH_PWM1, 4095)
    channel_off(CH_HEATER_1)
    channel_off(CH_HEATER_2)
    channel_off(CH_HEATER_3)
    channel_off(CH_HEATER_4)
    channel_off(CH_FAN_1)
    channel_off(CH_FAN_2)
    channel_off(CH_STEPPER_DIR)
    channel_off(CH_STEPPER_ENA)
    channel_off(CH_PU)
    channel_off(CH_SYS_LED)
except Exception as e:
    logger.error("Fatal: cannot set initial channel states (%s)", e)
    sys.exit(1)


try:
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
except (AttributeError, TypeError):
    client = mqtt.Client()

if MQTT_USER and MQTT_PASS:
    client.username_pw_set(MQTT_USER, MQTT_PASS)


device_info = {
    "identifiers": ["pca9685_pwm_controller"],
    "name": "PCA9685 PWM Controller",
    "model": "PCA9685",
    "manufacturer": "NXP Semiconductors",
    "sw_version": "0.1.0-fixed-channels",
}


def publish_discovery():
    discoveries = [
        ("switch", "pca_pwm1_enable", {
            "name": "PWM 1 Enable (CH0)",
            "unique_id": "pca_pwm1_enable",
            "command_topic": TOPIC_PWM1_ENABLE_CMD,
            "state_topic": TOPIC_PWM1_ENABLE_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("number", "pca_pwm1_duty", {
            "name": "PWM 1 Duty (CH0)",
            "unique_id": "pca_pwm1_duty",
            "command_topic": TOPIC_PWM1_DUTY_CMD,
            "state_topic": TOPIC_PWM1_DUTY_STATE,
            "availability_topic": AVAIL_TOPIC,
            "min": 0,
            "max": 100,
            "step": 1,
            "unit_of_measurement": "%",
            "mode": "slider",
            "device": device_info,
        }),
        ("switch", "pca_heater_1", {
            "name": "Heater 1 (CH1)",
            "unique_id": "pca_heater_1",
            "command_topic": TOPIC_HEATER_1_CMD,
            "state_topic": TOPIC_HEATER_1_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_heater_2", {
            "name": "Heater 2 (CH2)",
            "unique_id": "pca_heater_2",
            "command_topic": TOPIC_HEATER_2_CMD,
            "state_topic": TOPIC_HEATER_2_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_heater_3", {
            "name": "Heater 3 (CH3)",
            "unique_id": "pca_heater_3",
            "command_topic": TOPIC_HEATER_3_CMD,
            "state_topic": TOPIC_HEATER_3_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_heater_4", {
            "name": "Heater 4 (CH4)",
            "unique_id": "pca_heater_4",
            "command_topic": TOPIC_HEATER_4_CMD,
            "state_topic": TOPIC_HEATER_4_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_fan_1", {
            "name": "Fan 1 (CH5)",
            "unique_id": "pca_fan_1",
            "command_topic": TOPIC_FAN_1_CMD,
            "state_topic": TOPIC_FAN_1_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_fan_2", {
            "name": "Fan 2 (CH6)",
            "unique_id": "pca_fan_2",
            "command_topic": TOPIC_FAN_2_CMD,
            "state_topic": TOPIC_FAN_2_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("select", "pca_stepper_dir", {
            "name": "Stepper DIR (CH7)",
            "unique_id": "pca_stepper_dir",
            "command_topic": TOPIC_STEPPER_DIR_CMD,
            "state_topic": TOPIC_STEPPER_DIR_STATE,
            "availability_topic": AVAIL_TOPIC,
            "options": ["CW", "CCW"],
            "device": device_info,
        }),
        ("switch", "pca_stepper_ena", {
            "name": "Stepper ENA (CH8)",
            "unique_id": "pca_stepper_ena",
            "command_topic": TOPIC_STEPPER_ENA_CMD,
            "state_topic": TOPIC_STEPPER_ENA_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("switch", "pca_pu_enable", {
            "name": "PU Enable (CH9)",
            "unique_id": "pca_pu_enable",
            "command_topic": TOPIC_PU_ENABLE_CMD,
            "state_topic": TOPIC_PU_ENABLE_STATE,
            "availability_topic": AVAIL_TOPIC,
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": device_info,
        }),
        ("number", "pca_pu_freq_hz", {
            "name": "PU Frequency (CH9)",
            "unique_id": "pca_pu_freq_hz",
            "command_topic": TOPIC_PU_FREQ_CMD,
            "state_topic": TOPIC_PU_FREQ_STATE,
            "availability_topic": AVAIL_TOPIC,
            "min": 0,
            "max": 500,
            "step": 1,
            "unit_of_measurement": "Hz",
            "mode": "slider",
            "device": device_info,
        }),
    ]

    for component, unique_id, payload in discoveries:
        topic = f"homeassistant/{component}/{unique_id}/config"
        client.publish(topic, json.dumps(payload), retain=True)

    client.publish(AVAIL_TOPIC, "online", retain=True)

    client.publish(TOPIC_PWM1_ENABLE_STATE, "OFF", retain=True)
    client.publish(TOPIC_PWM1_DUTY_STATE, "0", retain=True)
    client.publish(TOPIC_HEATER_1_STATE, "OFF", retain=True)
    client.publish(TOPIC_HEATER_2_STATE, "OFF", retain=True)
    client.publish(TOPIC_HEATER_3_STATE, "OFF", retain=True)
    client.publish(TOPIC_HEATER_4_STATE, "OFF", retain=True)
    client.publish(TOPIC_FAN_1_STATE, "OFF", retain=True)
    client.publish(TOPIC_FAN_2_STATE, "OFF", retain=True)
    client.publish(TOPIC_STEPPER_DIR_STATE, stepper_dir, retain=True)
    client.publish(TOPIC_STEPPER_ENA_STATE, "OFF", retain=True)
    client.publish(TOPIC_PU_ENABLE_STATE, "OFF", retain=True)
    client.publish(TOPIC_PU_FREQ_STATE, str(int(pu_freq_hz)), retain=True)

    logger.info("Discovery messages published")


def on_connect(client, userdata, flags, reason_code, properties=None):
    rc = reason_code.value if hasattr(reason_code, "value") else reason_code
    if rc != 0:
        logger.error("MQTT connection failed with code %s", rc)
        return

    client.subscribe(TOPIC_PWM1_ENABLE_CMD)
    client.subscribe(TOPIC_PWM1_DUTY_CMD)
    client.subscribe(TOPIC_HEATER_1_CMD)
    client.subscribe(TOPIC_HEATER_2_CMD)
    client.subscribe(TOPIC_HEATER_3_CMD)
    client.subscribe(TOPIC_HEATER_4_CMD)
    client.subscribe(TOPIC_FAN_1_CMD)
    client.subscribe(TOPIC_FAN_2_CMD)
    client.subscribe(TOPIC_STEPPER_DIR_CMD)
    client.subscribe(TOPIC_STEPPER_ENA_CMD)
    client.subscribe(TOPIC_PU_ENABLE_CMD)
    client.subscribe(TOPIC_PU_FREQ_CMD)
    publish_discovery()

    logger.info("Connected to MQTT broker %s:%s", MQTT_HOST, MQTT_PORT)


def _payload_to_bool(payload: str) -> bool:
    return payload == "ON"


def on_message(client, userdata, msg):
    global pwm1_enabled, pwm1_value
    global heater_1, heater_2, heater_3, heater_4
    global fan_1, fan_2
    global pu_enabled, pu_freq_hz

    topic = msg.topic
    payload = msg.payload.decode("utf-8").strip()

    try:
        if topic == TOPIC_PWM1_ENABLE_CMD:
            new_state = _payload_to_bool(payload)
            with pwm1_lock:
                if new_state and not pwm1_enabled:
                    pwm1_value = float(DEFAULT_DUTY_CYCLE)
                pwm1_enabled = new_state
                update_pwm1_output_locked()
                client.publish(TOPIC_PWM1_ENABLE_STATE, "ON" if pwm1_enabled else "OFF", retain=True)
                client.publish(TOPIC_PWM1_DUTY_STATE, str(int(pwm1_value if pwm1_enabled else 0)), retain=True)

        elif topic == TOPIC_PWM1_DUTY_CMD:
            value = max(0.0, min(100.0, float(payload)))
            with pwm1_lock:
                pwm1_value = value
                pwm1_enabled = value > 0.0
                update_pwm1_output_locked()
                client.publish(TOPIC_PWM1_ENABLE_STATE, "ON" if pwm1_enabled else "OFF", retain=True)
                client.publish(TOPIC_PWM1_DUTY_STATE, str(int(value)), retain=True)

        elif topic == TOPIC_HEATER_1_CMD:
            heater_1 = _payload_to_bool(payload)
            apply_switch(CH_HEATER_1, heater_1)
            client.publish(TOPIC_HEATER_1_STATE, "ON" if heater_1 else "OFF", retain=True)

        elif topic == TOPIC_HEATER_2_CMD:
            heater_2 = _payload_to_bool(payload)
            apply_switch(CH_HEATER_2, heater_2)
            client.publish(TOPIC_HEATER_2_STATE, "ON" if heater_2 else "OFF", retain=True)

        elif topic == TOPIC_HEATER_3_CMD:
            heater_3 = _payload_to_bool(payload)
            apply_switch(CH_HEATER_3, heater_3)
            client.publish(TOPIC_HEATER_3_STATE, "ON" if heater_3 else "OFF", retain=True)

        elif topic == TOPIC_HEATER_4_CMD:
            heater_4 = _payload_to_bool(payload)
            apply_switch(CH_HEATER_4, heater_4)
            client.publish(TOPIC_HEATER_4_STATE, "ON" if heater_4 else "OFF", retain=True)

        elif topic == TOPIC_FAN_1_CMD:
            fan_1 = _payload_to_bool(payload)
            apply_switch(CH_FAN_1, fan_1)
            client.publish(TOPIC_FAN_1_STATE, "ON" if fan_1 else "OFF", retain=True)

        elif topic == TOPIC_FAN_2_CMD:
            fan_2 = _payload_to_bool(payload)
            apply_switch(CH_FAN_2, fan_2)
            client.publish(TOPIC_FAN_2_STATE, "ON" if fan_2 else "OFF", retain=True)

        elif topic == TOPIC_STEPPER_DIR_CMD:
            stepper_apply_dir(payload)
            client.publish(TOPIC_STEPPER_DIR_STATE, stepper_dir, retain=True)

        elif topic == TOPIC_STEPPER_ENA_CMD:
            stepper_apply_ena(_payload_to_bool(payload))
            client.publish(TOPIC_STEPPER_ENA_STATE, "ON" if stepper_ena else "OFF", retain=True)

        elif topic == TOPIC_PU_ENABLE_CMD:
            with pu_lock:
                pu_enabled = _payload_to_bool(payload)
            if pu_enabled:
                pu_start()
            client.publish(TOPIC_PU_ENABLE_STATE, "ON" if pu_enabled else "OFF", retain=True)

        elif topic == TOPIC_PU_FREQ_CMD:
            hz = max(0.0, float(payload))
            with pu_lock:
                pu_freq_hz = hz
            client.publish(TOPIC_PU_FREQ_STATE, str(int(hz)), retain=True)

    except Exception:
        logger.exception("Error processing topic=%s payload=%s", topic, payload)


client.on_connect = on_connect
client.on_message = on_message


def safe_shutdown(signum=None, frame=None):
    try:
        sys_led_stop()
        pu_stop()

        with pwm1_lock:
            pwm1_enabled = False
            pwm1_value = 0.0
            update_pwm1_output_locked()

        channel_off(CH_HEATER_1)
        channel_off(CH_HEATER_2)
        channel_off(CH_HEATER_3)
        channel_off(CH_HEATER_4)
        channel_off(CH_FAN_1)
        channel_off(CH_FAN_2)
        channel_off(CH_STEPPER_DIR)
        channel_off(CH_STEPPER_ENA)
        channel_off(CH_PU)
        channel_off(CH_SYS_LED)

        client.publish(AVAIL_TOPIC, "offline", retain=True)
        client.loop_stop()
        client.disconnect()
        pca.close()

    except Exception:
        logger.exception("Shutdown error")

    sys.exit(0 if signum is not None else 1)


signal.signal(signal.SIGTERM, safe_shutdown)
signal.signal(signal.SIGINT, safe_shutdown)


logger.info("Connecting to MQTT %s:%s...", MQTT_HOST, MQTT_PORT)
for attempt in range(1, 11):
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()
        time.sleep(2)
        if client.is_connected():
            break
        raise RuntimeError("Timeout")
    except Exception as e:
        if attempt == 10:
            logger.error("MQTT connect failed after retries (%s)", e)
            safe_shutdown()
        time.sleep(1.5 ** (attempt - 1))

sys_led_start()

logger.info("Service running")
logger.setLevel(logging.ERROR)

while True:
    time.sleep(5)
    if not client.is_connected():
        try:
            client.reconnect()
            client.publish(AVAIL_TOPIC, "online", retain=True)
        except Exception:
            logger.exception("MQTT reconnect failed")
