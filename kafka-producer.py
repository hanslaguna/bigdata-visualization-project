from kafka import KafkaProducer
from datetime import datetime, timedelta
import time
from json import dumps
import random

import time_uuid

# pip install kafka-python
# pip install time-uuid

KAFKA_TOPIC_NAME_CONS = "counts"  # topic
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    kafka_producer_1 = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                     value_serializer=lambda x: dumps(x).encode('utf-8'))
    sensor_list = ["CCTV_01", "CCTV_02", "CCTV_03", "CCTV_04", "CCTV_05", "CCTV_06", "CCTV_07", "CCTV_08", "CCTV_09", "CCTV_10"]
    message_list = []
    message = None
    add = 0
    date_today = datetime.now()


    for i in range(20000):
        i = i + 1

        message = {}
        print("Preparing message: " + str(i))

        # Hour Increases after 10 messages
        while i % 10 == 0:
            add =+ 1
            date_today += timedelta(hours=add)
            break
        # # Date increments after 500
        # while i % 5 == 0:
        #     add = + 1
        #     print("divisible by 5")
        #     date_today += timedelta(days=add)
        #     break

        in_car = random.randint(0, 4)
        in_bus = random.randint(0, 2)
        in_med_truck = random.randint(0, 2)
        in_large_truck = random.randint(0, 2)
        in_jeepney = random.randint(0, 2)
        in_bike = random.randint(0, 5)
        in_tryke = random.randint(0, 3)
        in_others = random.randint(0, 2)

        out_car = random.randint(0, 4)
        out_bus = random.randint(0, 2)
        out_med_truck = random.randint(0, 2)
        out_large_truck = random.randint(0, 2)
        out_jeepney = random.randint(0, 2)
        out_bike = random.randint(0, 5)
        out_tryke = random.randint(0, 3)
        out_others = random.randint(0, 2)

        sensor_id = random.choice(sensor_list)

        in_total = in_car + in_bus + in_med_truck + in_large_truck + in_jeepney + in_bike + in_tryke + in_others

        out_total = out_car + out_bus + out_med_truck + out_large_truck + out_jeepney + out_bike + out_tryke + out_others

        total = in_car + in_bus + in_med_truck + in_large_truck + in_jeepney + in_bike + in_tryke + in_others + out_car + out_bus + out_med_truck + out_large_truck + out_jeepney + out_bike + out_tryke + out_others

        # send data
        message["id"] = str(time_uuid.utctime())
        message["sensor_id"] = sensor_id
        message["date_saved"] = str(date_today.strftime('%Y-%m-%d'))
        message["time_saved"] = str(date_today.strftime("%H:%M:%S"))

        message["in_car"] = in_car
        message["in_bus"] = in_bus
        message["in_med_truck"] = in_med_truck
        message["in_large_truck"] = in_large_truck
        message["in_jeepney"] = in_jeepney
        message["in_bike"] = in_bike
        message["in_tryke"] = in_tryke
        message["in_others"] = in_others

        message["out_car"] = out_car
        message["out_bus"] = out_bus
        message["out_med_truck"] = out_med_truck
        message["out_large_truck"] = out_large_truck
        message["out_jeepney"] = out_jeepney
        message["out_bike"] = out_bike
        message["out_tryke"] = out_tryke
        message["out_others"] = out_others

        message["in_total"] = in_total
        message["out_total"] = out_total
        message["count_total"] = total

        print("Message: ", message)

        # message_list.append(message)
        kafka_producer_1.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)  # sleep every second

    # print(message_list)

    print("Kafka Producer Application Completed. ")