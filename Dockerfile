services:

  storage-control:
    image: storage_manager_arm64:1.0
    container_name: opendash_storage_manager
    env_file:
      - .env
    user: "0:0"
    restart: always
    volumes:
      - ${dest}:${dest}
#    depends_on:
#        kafka:
#          condition: service_healthy
    networks:
      - kafka_opendash


  Opendash_ui:
    image: opendash_arm64:1.2
    container_name: opendash_webui
    env_file:
      - .env
    ports:
      - "8001:8000"
    restart: always
    depends_on:
      postgres_db:
        condition: service_healthy
#      kafka:
#        condition: service_healthy
          
    networks:
      - kafka_opendash

  nginx:
    image: nginx
    container_name: opendash_vid_hosting_nginx
    restart: always
    ports:
      - "9876:9876"
    networks:
      - kafka_opendash
    volumes:
      - ./default.conf:/etc/nginx/conf.d/default.conf:ro
      - ${dest}:/recordings/



  postgres_db:
    image: postgres:16
    restart: always
    container_name: opendash_db
    env_file:
      - .env
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
    networks:
      - kafka_opendash
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5
  
  redis:
    image: redis:7-alpine
    container_name: opendash_redis_broker
    ports:
      - "6379:6379"
    restart: always
    networks:
      - kafka_opendash

  # kafka:
  #   image: apache/kafka:latest
  #   container_name: broker
  #   healthcheck:
  #     test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
  #     interval: 10s
  #     timeout: 5s
  #     retries: 5
  #   ports:
  #     - "9092:9092"
  #     - "9094:9094"
  #   environment:
  #     KAFKA_NODE_ID: 1
  #     KAFKA_PROCESS_ROLES: broker,controller
  #     KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
  #     KAFKA_ADVERTISED_LISTENERS: EXTERNAL://localhost:9094, INTERNAL://broker:9092
  #     KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
  #     KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT, EXTERNAL:PLAINTEXT, CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
  #     KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:9093
  #     KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
  #     KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  #     KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
  #     KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
  #     KAFKA_NUM_PARTITIONS: 3
  #     KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
  #   networks:
  #     - opendash







volumes:
  # records:
  pg_data:
  
networks:
  kafka_opendash:
     external: true