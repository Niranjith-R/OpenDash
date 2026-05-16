import os
import logging
import sys
import asyncio
from datetime import datetime
import time
from confluent_kafka import Producer
import socket
import psycopg


'''
=====================================================================================
Config
'''
db_host = os.getenv('db_host')
db_port = os.getenv("db_port")
db_passwd = os.getenv('db_pass')
db_username = os.getenv("db_usrname")
kafka_server = os.getenv('kafka_server')


conf = {
    'bootstrap.servers' : str(kafka_server),
    'client.id': socket.gethostname()
}

producer = Producer(conf)
topic = "Storage"

def acked(err, msg):
    if err is not None:
        logger.error(f"Failed to deliver message : {msg} : {err}")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("Storage.log")])
logger.info("Loading up Storage Daemon")
producer.produce(topic, value=f'Loading up Storage Daemon', on_delivery=acked)


max_size = int(os.getenv("max-size", 15))
sleep = int(os.getenv("sleep", 900))
logger.info(f"Max size = {max_size} GB")
producer.produce(topic, value=f"Max size = {max_size} GB", on_delivery=acked)
try:
    #Use this while in Production
    #output_dest = os.path.expanduser("~/recordings/")
    output_dest = os.getenv("dest", os.path.expanduser("~/recordings/"))
except Exception as e:
    logger.error(f"Exception occured in output destination {e}")
    producer.produce(topic, value=f"Exception occured in output destination {e}", on_delivery=acked)

days = int(os.getenv("day-limit", 10))
logger.info(f"Vids will be deleted after {days} days")
producer.produce(topic, value=f"Vids will be deleted after {days} days", on_delivery=acked)

'''
=====================================================================================
'''


def get_size(desination):
    size = 0
    for root, dirs, files in os.walk(desination):
        for i in files:
            fp = os.path.join(root, i)
            logger.info(f'Checking file size of {fp}')
            producer.produce(topic, value=f'Checking file size of {fp}', on_delivery=acked)
            size+= os.path.getsize(fp)
    gb = size / (1024**3)
    return gb

def get_dates(dest):
    storage_mapping = dict()
    directories = list()
    with os.scandir(dest) as e:
        count_dir = count_file = 0
        for i in e:
            if i.is_dir():
                directories.append(i)
                # logger.info(f"Found Directory {i}")
                count_dir+=1
            elif i.is_file():
                # logger.info(f"found a file {i}")
                count_file+=1
                storage_mapping[i.name] = i.path
    for i in directories:
        chile_mapping = get_dates(i.path)
        storage_mapping.update(chile_mapping)
    logger.info(f"found {count_dir} Directories with total of {count_file} files")
    producer.produce(topic, value=f"found {count_dir} Directories with total of {count_file} files", on_delivery=acked)
    producer.flush()
    return storage_mapping

async def purge(dest):
    try:
        conn=psycopg.connect(host=db_host, dbname="Opendash", user=db_username, password=db_passwd, port=str(db_port))
        curse = conn.cursor()
    except Exception as e:
        logger.error(f"Exception while conencting to DB : {e}")

    dest_manipulation = dest.split("/")
    file_name = dest_manipulation[-1]
    SQL="UPDATE public.main_video SET is_deleted=true where video_name=%s"
    curse.execute(SQL, [file_name])
    conn.commit()

    try:
        await asyncio.to_thread(os.remove, dest)
        logger.info(f"successfully deleted file with path{dest}")
        producer.produce(topic, value=f"successfully deleted file with path{dest}", on_delivery=acked)
        producer.flush()
    except Exception as e:
        logger.error(f"Deleting {dest} failed!, {e}")
        producer.produce(topic, value=f"Deleting {dest} failed!, {e}", on_delivery=acked)
        producer.flush()
    
async def storage_manager(day):
    this_moment = time.time()
    x_days_ago = this_moment - 86400 * day
    x_days_ago_obj = datetime.fromtimestamp(x_days_ago)
    while True:
        logger.info(f"Monitoring File Sizes in the folder {output_dest}")
        producer.produce(topic, value=f"Monitoring File Sizes in the folder {output_dest}", on_delivery=acked)
        file_size = get_size(output_dest)
        logger.info(f'Current size = {file_size}')
        producer.produce(topic, value=f'Current size = {file_size}', on_delivery=acked)
        if file_size > max_size:
            logger.info("File Size exceeded, Purging Old files")
            producer.produce(topic, value=f"File Size exceeded, Purging Old files", on_delivery=acked)
            files_with_path = get_dates(output_dest)
            file_date = files_with_path.keys() 
            for i in file_date:
                try:
                    dateobj = datetime.strptime(i.strip()[:-4], "%d-%m-%Y %H:%M:%S")
                except Exception as e:
                    logger.error(f"Fatal error in file name {e}")
                    producer.produce(topic, value=f"Fatal error in file name {e}", on_delivery=acked)
                    continue
                if dateobj < x_days_ago_obj :
                    logger.info(f"The file {i} with path {files_with_path[i]} will be deleted")
                    producer.produce(topic, value=f"The file {i} with path {files_with_path[i]} will be deleted", on_delivery=acked)
                    await purge(files_with_path[i])
                    producer.flush()
        else:
            logger.info("File size is under control")
            producer.produce(topic, value=f"File size is under control", on_delivery=acked)
            producer.flush()
        await asyncio.sleep(sleep)


async def main():
    await storage_manager(day=days)


if __name__ == '__main__':
    asyncio.run(main())
