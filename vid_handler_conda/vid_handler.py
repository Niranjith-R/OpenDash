import cv2 as cv
import time
import os
import logging
from multiprocessing import Pool
import sys
import psycopg
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaException
from time import sleep
import socket



logger = logging.getLogger(__name__)
logging.basicConfig( level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout),])
# logging.basicConfig( level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("applifecycle.log")])



'''
===========================================================================================================================
ENV
===========================================================================================================================

'''

db_host = os.getenv('db_host')
db_username = os.getenv("db_usrname")
db_passwd = os.getenv('db_pass')
db_port = os.getenv("db_port")
kafka_server = os.getenv('kafka_server')
# kafka_server = 'localhost:9094'



output_dest = os.getenv("dest", os.path.expanduser("~/recordings/"))

max_size = os.getenv("max-size", 1)

vid_length = os.getenv("vid_length", 60)

width, height = 1920, 720
fps = 30

if not os.path.exists(output_dest):
    logger.info("Base path does not exits!")
    try:
        os.mkdir(output_dest)
        logger.info("Base Path created successfully")
    except Exception as e:
        logger.error(f"Exception occured while creating Base Path {e}")

local_now = time.localtime()
local_today =   time.strftime("%d-%m-%Y_%H-%M-%S", local_now)

def acked(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message to Kafka, {err}")


'''
===========================================================================================================================
ENDENV
===========================================================================================================================

'''




'''
===========================================================================================================================
config
===========================================================================================================================
'''


now = time.localtime()
today = str(now.tm_mday)+ "-" + str(now.tm_mon) + '-' + str(now.tm_year) + ' ' + str(now.tm_hour) + ':' + str(now.tm_min) + ':' + str(now.tm_sec)
sql='INSERT INTO main_video(video_name, path, camera_name, created_date, is_deleted) VALUES (%s, %s, %s, %s, %s)'
Output=False
conf = {
    "bootstrap.servers" : str(kafka_server),
    'client.id' : socket.gethostname(),
}

# producer = Producer(conf)
topic = "vid_recorder"
cons_topic = ["Dragonfly"]


'''
===========================================================================================================================
END CONFIG
===========================================================================================================================
'''





def list_available_camera():
    # conf = {
    # "bootstrap.servers" : "0.0.0.0:9092",
    # 'client.id' : socket.gethostname()
    # }
    producer = Producer(conf)
    logger.info("Searching for Available Camera")
    producer.produce(topic, value = "Searching for Available Camera", on_delivery=acked)
    producer.poll(0)
    available_camera = []
    i=0
    while True:
        cap = cv.VideoCapture(i)
        if cap.isOpened():
            available_camera.append(i)
            logger.info("Usable camera found")
            producer.produce(topic, value = "Usable camera found", on_delivery=acked)
            producer.poll(0)
            cap.release()
            i+=1
        else:
            break   
    logger.info("Finished searching for cameras")
    producer.produce(topic, value = "Finished searching for cameras", on_delivery=acked)
    producer.poll(0)
    producer.flush()
    return available_camera

def record_ip_cam_vid(cam):

    run_state = True

    # conf = {
    # "bootstrap.servers" : "0.0.0.0:9092",
    # 'client.id' : socket.gethostname()
    # }
    producer = Producer(conf)
    topic = "vid_recorder"

    cons_conf = {
    "bootstrap.servers" : str(kafka_server),
    'client.id' : socket.gethostname(),
    'group.id' : 'vid_ip',
    'auto.offset.reset' : 'latest'
    }

    cam_consumer = Consumer(cons_conf)
    cam_consumer.subscribe(cons_topic)


    try:
        conn=psycopg.connect(host=str(db_host), dbname="Opendash", user=str(db_username), password=str(db_passwd), port=str(db_port))
        logger.info("Connected to Database Successfulyy")
        producer.produce(topic, value = "Connected to Database Successfulyy", on_delivery=acked)
        producer.poll(0)
        curse = conn.cursor()
    except Exception as e:
        logger.error(f"Could not connect with the Database {e}")
        producer.produce(topic, value = f"Could not connect with the Database {e}", on_delivery=acked)
        producer.poll(0)


    global local_today 
    logger.info("Starting up Video Recorder for IP camera")
    producer.produce(topic, value = f"Starting up Video Recorder for IP camera", on_delivery=acked)
    producer.poll(0.1)
    cap = cv.VideoCapture(cam)#This Selects The Video Input Device
    if not cap.isOpened():
        logger.warning("Cannot Open Camera")
        producer.produce(topic, value = f"Cannot Open Camera", on_delivery=acked)
        producer.poll(0)
        

    cam_name = cam

    for i in cam:
            if i in [":", "*", "/", "\\", ".", "\n"]:
                cam = cam.replace(i, "")


    frame_height = int(cap.get(cv.CAP_PROP_FRAME_HEIGHT))
    out = create_writer(cap=cap, cam=cam)
    start_time = time.time()
    try:
        while run_state:
            realtime_handler = time.localtime()
            realtime = str(realtime_handler.tm_mday)+ "-" + str(realtime_handler.tm_mon) + '-' + str(realtime_handler.tm_year) + '--' + str(realtime_handler.tm_hour) + ':' + str(realtime_handler.tm_min) + ':' + str(realtime_handler.tm_sec)
            ret, frame = cap.read()# 'ret' retuns True if the frame is recieved
            if realtime_handler.tm_hour > 18 or realtime_handler.tm_hour < 6 :
                cv.putText(frame, realtime, (30, frame_height-50), fontFace = cv.FONT_HERSHEY_DUPLEX,color=(255,255,255) ,fontScale=0.5, thickness=1)
            else:
                cv.putText(frame, realtime, (30, frame_height-50), fontFace = cv.FONT_HERSHEY_DUPLEX,color=(0,0,0) ,fontScale=0.5, thickness=1)
            end_time = time.time()
            cmd = cam_consumer.poll(timeout = 0)
            if cmd:
                logger.info(f"CMD recieved {cmd.value().decode('utf-8')}")
                cmd_value = cmd.value().decode('utf-8')
                if cmd_value == "RESTART_IP":
                    logger.info("Command for Restart in record_ip_cam")
                    cam_consumer.commit(message=cmd)
                    cam_consumer.close()
                    run_state = False
                    break
                elif "ADD" in cmd_value:
                    cmd = cmd.split()
                    cam = cmd[-1]
                    logger.info(f"command to add {cam} to the list")
                    producer.produce(topic, value = f"Can't Recieve frame (Steam end ?)", on_delivery=acked)
                    producer.poll(0)
                    ip_camera(add=cam)

            if not ret :
                logger.error("Can't Recieve frame (Steam end ?)")
                producer.produce(topic, value = f"Can't Recieve frame (Steam end ?)", on_delivery=acked)
                producer.poll(0)
            else:
                # This writes the Frame
                out.write(frame)
                if Output == True:
                    cv.imshow(f"Video Feed of {cam}",frame)
                    if cv.waitKey(1)==ord('q'):#ord('q') checkes if the key 'q' is pressed
                        logger.info(f"Ending recording of {cam} (Manual intervention)")
                        break
            if end_time - start_time >= 60:
                logger.info(f"Releasing file : {out}")
                producer.produce(topic, value = f"Releasing file : {out}", on_delivery=acked)
                producer.poll(0)
                out.release()
                cam_path = os.path.join(output_dest, str(cam))
                dt = datetime.strptime(local_today, "%d-%m-%Y_%H-%M-%S")
                formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
                ip_path=str(cam)
                sql_params = (f"{local_today}.mp4", f"{ip_path}/{local_today}.mp4", cam_name, formatted, "FALSE")
                try:
                    curse.execute(sql, sql_params)
                    conn.commit()
                    logger.info("Successfully written to DB")
                    producer.produce(topic, value = f"Successfully written to DB", on_delivery=acked)
                    producer.poll(0)
                except Exception as e:
                    logger.error(f"Error while saving to Database: {e}")
                    producer.produce(topic, value = f"Error while saving to Database: {e}", on_delivery=acked)
                    producer.poll(0)

                new_local_now = time.localtime()
                local_today = time.strftime("%d-%m-%Y_%H-%M-%S", new_local_now)

                out = create_writer(cap=cap, cam=cam)
                start_time = time.time()
    except Exception as e:
        logger.error(f"Error while saving content to file : {e}")
        producer.produce(topic, value = f"Error while saving content to file : {e}", on_delivery=acked)
        producer.poll(0)
        cap.release()
        cv.destroyAllWindows()
    producer.flush()

def record_vid(cam):

    run_state = True

    # conf = {
    # "bootstrap.servers" : "0.0.0.0:9092",
    # 'client.id' : socket.gethostname()
    # }
    producer = Producer(conf)
    topic = "vid_recorder"

    cons_conf = {
    "bootstrap.servers" : str(kafka_server),
    'client.id' : socket.gethostname(),
    'group.id' : 'vid_usb',
    'auto.offset.reset' : 'latest'
    }

    cam_consumer = Consumer(cons_conf)
    cam_consumer.subscribe(cons_topic)

    try:
        conn=psycopg.connect(host=str(db_host), dbname="Opendash", user=str(db_username), password=db_passwd, port=str(db_port))
        logger.info("Connected to Database Successfulyy")
        producer.produce(topic, value = f"Connected to Database Successfulyy", on_delivery=acked)
        producer.poll(0.1)
        curse = conn.cursor()
    except Exception as e:
        logger.error(f"Could not connect with the Database {e}")
        producer.produce(topic, value = f"Could not connect with the Database {e}", on_delivery=acked)
        producer.poll(0)

    global local_today 
    logger.info("Starting up Video Recorder")
    producer.produce(topic, value = f"Starting up Video Recorder", on_delivery=acked)
    producer.poll(0)
    # cap = cv.VideoCapture(cam, cv.CAP_V4L2)#This Selects The Video Input Device
    cap = cv.VideoCapture(cam)
    if not cap.isOpened():
        logger.warning("Cannot Open Camera")
        producer.produce(topic, value = f"Cannot Open Camera", on_delivery=acked)
        producer.poll(0)
        exit()

    
    frame_height = int(cap.get(cv.CAP_PROP_FRAME_HEIGHT))
    out = create_writer(cap=cap, cam=cam)
    start_time = time.time()
    try:
        while run_state:
            realtime_handler = time.localtime()
            realtime = str(realtime_handler.tm_mday)+ "-" + str(realtime_handler.tm_mon) + '-' + str(realtime_handler.tm_year) + '--' + str(realtime_handler.tm_hour) + ':' + str(realtime_handler.tm_min) + ':' + str(realtime_handler.tm_sec)
            ret, frame = cap.read()# 'ret' retuns True if the frame is recieved
            if realtime_handler.tm_hour > 18 or realtime_handler.tm_hour < 6 :
                cv.putText(frame, realtime, (30, frame_height-50), fontFace = cv.FONT_HERSHEY_DUPLEX,color=(255,255,255) ,fontScale=0.5, thickness=1)
            else:
                cv.putText(frame, realtime, (30, frame_height-50), fontFace = cv.FONT_HERSHEY_DUPLEX,color=(0,0,0) ,fontScale=0.5, thickness=1)
            end_time = time.time()
            if not ret :
                logger.error("Can't Recieve frame (Steam end ?)")
                producer.produce(topic, value = f"Can't Recieve frame (Steam end ?)", on_delivery=acked)
                producer.poll(0)
            else:
                # This writes the Frame
                out.write(frame)
                cmd = cam_consumer.poll(timeout = 0)
                if cmd:
                    logger.info(f"CMD recieved {cmd.value().decode('utf-8')}")
                    if cmd.value().decode('utf-8') == "RESTART_USB":
                        logger.info("Command for Restart in Record_Vid")
                        cam_consumer.commit(message=cmd)
                        cam_consumer.close()
                        run_state = False
                elif Output == True:
                    cv.imshow(f"Video Feed of {cam}",frame)
                    if cv.waitKey(1)==ord('q'):#ord('q') checkes if the key 'q' is pressed
                        logger.info(f"Ending recording of {cam} (Manual intervention)")
                        break
            if end_time - start_time >= 60:
                logger.info(f"Releasing file : {out}")
                producer.produce(topic, value = f"Releasing file : {out}", on_delivery=acked)
                producer.poll(0)
                out.release()
                cam_path = os.path.join(output_dest, str(cam))
                dt = datetime.strptime(local_today, "%d-%m-%Y_%H-%M-%S")
                formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
                sql_params = (f"{local_today}.mp4", f'{str(cam)}/{local_today}.mp4', cam, formatted, "FALSE")
                try:
                    curse.execute(sql, sql_params)
                    conn.commit()
                    logger.info("Successfully written to DB")
                    producer.produce(topic, value = f"Successfully written to DB", on_delivery=acked)
                    producer.poll(0)
                except Exception as e:
                    logger.error(f"Error while saving to Database: {e}")
                    producer.produce(topic, value = f"Error while saving to Database: {e}", on_delivery=acked)
                    producer.poll(0)

                new_local_now = time.localtime()
                local_today = time.strftime("%d-%m-%Y_%H-%M-%S", new_local_now)

                out = create_writer(cap=cap, cam=cam)
                start_time = time.time()
    except Exception as e:
        logger.error(f"Error while saving content to file : {e}")
        producer.produce(topic, value = f"Error while saving content to file : {e}", on_delivery=acked)
        producer.poll(0)
        cap.release()
        cv.destroyAllWindows()
    producer.flush()

def create_writer(cap, cam):
    # conf = {
    # "bootstrap.servers" : "0.0.0.0:9092",
    # 'client.id' : socket.gethostname()
    # }
    producer = Producer(conf)
    topic = "vid_recorder"
    global local_today
    logger.info(f"Creating Video Writer for cam {cam}")
    producer.produce(topic, value = f"Creating Video Writer for cam {cam}", on_delivery=acked)
    producer.poll(0)
    if type(cam) is str:
        for i in cam:
            if i in [":", "*", "/", "\\", ".", "\n"]:
                cam = cam.replace(i, "")
    cam_path = os.path.join(output_dest, str(cam))
    if not os.path.exists(cam_path):
        logger.info(f"path Does not Exists, Creating New Path {cam_path}")
        producer.produce(topic, value = f"path Does not Exists, Creating New Path {cam_path}", on_delivery=acked)
        producer.poll(0)
        os.mkdir(cam_path.strip())
    else:
        logger.info("Path exists")
        producer.produce(topic, value = f"Path exists", on_delivery=acked)
        producer.poll(0)
        logger.info(f"using path {cam_path}")
        producer.produce(topic, value = f"using path {cam_path}", on_delivery=acked)
        producer.poll(0)

    fourcc = cv.VideoWriter_fourcc(*"avc1")
    frame_width = int(cap.get(cv.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv.CAP_PROP_FRAME_HEIGHT))
    # fps = int(cap.get(cv.CAP_PROP_FPS)) or 30
    logger.info(f"New file with name {cam_path}/{local_today}.mp4 Created")
    producer.produce(topic, value = f"New file with name {cam_path}/{local_today}.mp4 Created", on_delivery=acked)
    producer.poll(0)
    producer.flush()
    return cv.VideoWriter(f'{cam_path}/{local_today}.mp4', fourcc, 24,(frame_width, frame_height))

def ip_camera(add=""):
    producer = Producer(conf)
    config_path = os.path.join(output_dest, "config")
    if not os.path.exists(config_path):
        logger.warning("Config does not exist")
        producer.produce(topic, value = f"Config does not exist", on_delivery=acked)
        producer.poll(0)
        os.mkdir(config_path)

    config_file_path = os.path.join(config_path, "config.conf")
    if not os.path.exists(config_file_path):
        logger.warning("config file does not exist")
        producer.produce(topic, value = f"config file does not exist", on_delivery=acked)
        producer.poll(0)
        f = open(config_file_path, "w")
        f.close()

    elif add:
        with open(config_file_path, 'a') as f:
            f.write(add + '\n')
        sleep(1)
        logger.info(f"Successfully added ip_camera {add}")
        producer.produce(topic, value = f"Successfully added ip_camera {add}", on_delivery=acked)
        producer.flush()
    
    else:
        f = open(config_file_path, "r")
        data = f.readlines()
        if not data:
            logger.warning("There are no IP Cameras found, Add them!")
            producer.produce(topic, value = f"There are no IP Cameras found, Add them!", on_delivery=acked)
            producer.poll(0)
            return data
        else:
            return data
    producer.flush()






def main():
    producer = Producer(conf)
    logger.info("Starting up service")
    producer.produce(topic, value = f"Starting up service", on_delivery=acked)
    producer.poll(0)
    logger.info(f"Today is {today}")
    producer.produce(topic, value = f"Today is {today}", on_delivery=acked)
    producer.poll(0)
    camera = list_available_camera()
    ip_cams = ip_camera()
    if ip_cams:
        total = len(camera) + len(ip_cams)
    else:
        total = len(camera)

    if total:
        P = Pool(processes=total)
        for cam in camera:
            P.apply_async(record_vid, [cam])
            logger.info(f"Launched worker for local camera {cam}")
            producer.produce(topic, value = f"Launched worker for local camera {cam}", on_delivery=acked)
            producer.poll(0)
        if ip_cams:
            created = list()
            for ip_cam in ip_cams:
                if ip_cam not in created:
                    P.apply_async(record_ip_cam_vid, [ip_cam])
                    logger.info(f"Launched worker for IP camera {ip_cam}")
                    producer.produce(topic, value = f"Launched worker for IP camera {ip_cam}", on_delivery=acked)
                    producer.poll(0)
                    created.append(ip_cam)

        P.close()
        logger.info("All recording Services Started")
        producer.produce(topic, value = f"All recording Services Started", on_delivery=acked)
        producer.poll(0)
        P.join()
        logger.info("Recording Services Stopped")
        producer.produce(topic, value = f"Recording Services Stopped", on_delivery=acked)
        producer.poll(0)
    else:
        logger.info("There are no cameras found. Add it")
        producer.produce(topic, value = f"There are no cameras found. Add it", on_delivery=acked)
        producer.produce(topic, value = f"listening for Commands", on_delivery=acked)
        logger.info("listening for Commands")
        producer.produce(topic, value = f"add new cameras by either plugging into USB or use command ADD <ip_camera_address>", on_delivery=acked)
        logger.info("add new cameras by either plugging into USB or use command ADD <ip_camera_address>")
        producer.produce(topic, value = f"When finished. enter \"EXIT\" ", on_delivery=acked)
        logger.info("When finished. enter \"EXIT\" to restart ")
        producer.flush()

        cons_conf = {
        "bootstrap.servers" : str(kafka_server),
        'client.id' : socket.gethostname(),
        'group.id' : 'vid_usb',
        'auto.offset.reset' : 'latest'
                    }

        cam_consumer = Consumer(cons_conf)
        cam_consumer.subscribe(cons_topic)
        cmd_value = ""
        while cmd_value != "EXIT":
            cmd = cam_consumer.poll(timeout=0.3)
            if cmd:
                cmd_value = cmd.value().decode("utf-8")
                logger.info(f"CMD recieved {cmd_value}")
                if "ADD" in cmd_value:
                    cmd = cmd_value.split()
                    cam = cmd[-1]
                    logger.info(f"command to add {cam} to the list")
                    producer.produce(topic, value = f"Command to add {cam} to the list", on_delivery=acked)
                    sleep(1)
                    ip_camera(add=cam)
                    producer.flush()
        cam_consumer.close()


if __name__ == "__main__":
    main()


