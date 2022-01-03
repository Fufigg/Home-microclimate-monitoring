import random
import time
import paho.mqtt.client as mqtt
from config import username, password, broker, port
from database import write_to_database

client_id = f'python-mqtt-{random.randint(0, 1000)}'


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("/hmc/+/temperature")
    client.subscribe("/hmc/+/humidity")
    client.subscribe("/hmc/+/co2")
    client.subscribe("/hmc/+/atmospheric_pressure")


def on_message(client, userdata, msg):
    _, _, device, parameter = msg.topic.split('/')
    write_to_database(device, parameter, float(msg.payload.decode()))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set(username, password)
client.connect(broker, port)

client.loop_forever()
while True:
    time.sleep(1)
