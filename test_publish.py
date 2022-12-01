import random
import time

import paho.mqtt.client as mqtt_client

# broker 정보 #1
broker_address = "localhost"
broker_port = 1883

topic = "/python/mqtt"


def connect_mqtt() -> mqtt_client.Client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker")
        else:
            print(f"Failed to connect, Returned code: {rc}")

    def on_disconnect(client, userdata, flags, rc=0):
        print(f"disconnected result code {str(rc)}")

    def on_log(client, userdata, level, buf):
        print(f"log: {buf}")

    # client 생성 #2
    client_id = f"mqtt_client_{random.randint(0, 1000)}"
    client = mqtt_client.Client(client_id)

    # 콜백 함수 설정 #3
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log

    # ID/PW 설정 #4(생성 안했을시 주석)
    client.username_pw_set("사용자 아이디", "사용자 비번")

    # broker 연결 #5
    client.connect(host=broker_address, port=broker_port)

    return client


def publish(client: mqtt_client.Client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1


def run():
    client = connect_mqtt()
    client.loop_start()  # 5
    print(f"connect to broker {broker_address}:{broker_port}")
    publish(client)  # 6


if __name__ == '__main__':
    run()

'''
#1 broker 정보 입력하기
RabbitMQ를 docker로 띄운 상태입니다.
포트 포워딩을 통해서 local의 1883을 docker의 1883으로 연결했습니다.
#4에서 client를 broker로 연결합니다.

#2 client 생성
Client 생성자는 4개의 파라미터를 받습니다. 여기서 client_id는 필수값이고, 또한 유니크한 값이여야합니다.

Client(client_id=””, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
'''
