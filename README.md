# OpenDash
> A first year project of a B.Tech student

OpenDash solves the problem with traditional dashcams — you buy one and you're stuck with it. Dashcams are expensive and often the quality offered does not justify the price. OpenDash is a **microservices-based dashcam system** designed to run on an SBC (Single Board Computer), giving you full control over your recordings.

## Components

- **Video Recorder** — Captures and segments video from USB or IP cameras
- **Storage Manager** — Keeps storage usage under control automatically
- **WebUI** — Django-based interface to view and download recordings

Most components can be deployed using Docker Compose. However, it is **highly recommended to run Video Recorder natively**.

>  AI used only for debugging

---

## Demo


---

## Video Recorder

Video Recorder uses **OpenCV** to record video from a local USB camera or IP camera. It records video into segments of `n` seconds, saves them locally, and sends details to **Postgres**. It also uses **Apache Kafka** to stream logs in real-time to the WebUI.

Although it can be run in a Docker container, it is highly recommended to run it as a **systemd service natively**, as it will start recording on system boot and integrates with camera hardware better.

### Environment Variables

| Variable | Description |
|---|---|
| `db_host` | Database host name |
| `db_usrname` | Username for database authentication |
| `db_pass` | Password for database authentication |
| `db_port` | Port for database communication |
| `kafka_server` | `host:port` of Kafka server |
| `dest` | *(Optional)* Destination for recorded videos. Defaults to `~/recording` |
| `vid_length` | Maximum length of each video segment |

### Dependencies
`Python` `OpenCV` `Psycopg` `Confluent_Kafka`

---

## Storage Manager

Storage Manager is an **async Python script** that keeps the recording folder size under control. By default, it checks the directory every 15 minutes. If the folder exceeds the size limit, it deletes files older than 10 days.

It also streams its logs to the WebUI via **Apache Kafka**.

This can be run in Docker by taking advantage of bind volumes.

### Environment Variables

| Variable | Description |
|---|---|
| `db_host` | Database host name |
| `db_usrname` | Username for database authentication |
| `db_pass` | Password for database authentication |
| `db_port` | Port for database communication |
| `kafka_server` | `host:port` of Kafka server |
| `max-size` | Maximum size of recording folder. Defaults to `15GB` |
| `sleep` | Seconds between each run |
| `dest` | *(Optional)* Destination for recorded videos. Defaults to `~/recording` |
| `day-limit` | Files older than this (in days) are deleted when size limit is exceeded |

### Dependencies
`Python` `Psycopg` `Confluent_Kafka`

---

## WebUI

OpenDash WebUI uses **Django** for both backend and frontend, with **Django Templating Language** for the UI. It uses **Postgres** for the database, **Daphne** and **Redis** for Django Channels, which handles WebSockets to deliver real-time logs from Apache Kafka.

The WebUI is designed to be **single-user**. On first boot, the user is prompted to create a username and password. Once signed in, recordings can be viewed in chronological order and downloaded if needed.

Recordings are served via an **Nginx** server.

Since this requires two separate commands to run (one for the WebUI and one for the Kafka consumer), **Supervisor** is used inside the Docker container to run them in parallel.

An example Docker Compose file has been included in the repository.

### Environment Variables

| Variable | Description |
|---|---|
| `redis_host` | Host of Redis server |
| `redis_port` | Port of Redis server |
| `db_host` | Database host name |
| `db_usrname` | Username for database authentication |
| `db_pass` | Password for database authentication |
| `db_port` | Port for database communication |
| `kafka_server` | `host:port` of Kafka server |

---

## Contact

Got any questions or suggestions? Write to me at [niranjith.pro@gmail.com](mailto:niranjith.pro@gmail.com)
