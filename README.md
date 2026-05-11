# Лабораторна робота №3-5 (Kafka Streams, Java)

## Опис
Проєкт реалізує потокову обробку даних поїздок за допомогою Apache Kafka Streams.

Продюсер зчитує дані з CSV-файлу та надсилає повідомлення у Kafka.  
Kafka Streams застосунок виконує агрегацію поїздок за датою та записує результати в окремі Kafka-топіки.

## Технології
- Java 17
- Apache Kafka Streams
- Maven
- Docker, Docker Compose
- Apache Kafka (Confluent)
- Kafka UI
- Apache Iceberg
- Apache Polaris
- Trino
- MinIO

## Структура проєкту
```text
Lab_3-5/
├─ docker-compose.yml
├─ Divvy_Trips_2019_Q4.csv
├─ README.md
├─ scripts/
│  └─ setup-polaris.ps1
├─ trino/
│  ├─ catalog/
│  │  └─ iceberg.properties
│  └─ demo-iceberg.sql
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
└─ kafka-streams-consumer/
   ├─ Dockerfile
   ├─ pom.xml
   └─ src/main/java/com/lab_3_5/streaming/StreamsApplication.java
```

# Kafka Topics

## Вхідні топіки

- `Topic1`
- `Topic2`

## Вихідні топіки

- `trip-avg-duration-by-day`
- `trip-count-by-day`
- `trip-top-start-station-by-day`
- `trip-top3-stations-by-day`

```

## Обчислення:

### 1. Середня тривалість поїздки за день
Output topic:

```text
trip-avg-duration-by-day
```

---

### 2. Кількість поїздок за день
Output topic:

```text
trip-count-by-day
```

---

### 3. Найпопулярніша початкова станція за день
Output topic:

```text
trip-top-start-station-by-day
```

---

### 4. Топ-3 станції за день
Враховуються:
- `from_station_name`
- `to_station_name`

Output topic:

```text
trip-top3-stations-by-day
```

---

## Запуск
```bash
docker compose up --build
```

Або у фоні:
```bash
docker compose up -d --build
```

Після запуску доступні сервіси:
- Trino Web UI: `http://localhost:8080`
- Kafka UI: `http://localhost:8088`
- MinIO UI: `http://localhost:9001` (`admin` / `password`)
- MinIO API: `http://localhost:9000`
- Polaris API: `http://localhost:8181`


## Налаштування Iceberg Lakehouse
Конфігурація Trino для Iceberg REST catalog знаходиться у файлі `trino/catalog/iceberg.properties`.

Після старту контейнерів створити каталог Polaris і ролі доступу можна командою:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-polaris.ps1
```

Після цього можна виконати демонстраційні SQL-запити в Trino:
```powershell
docker compose exec trino trino --server localhost:8080 --catalog iceberg -f /etc/trino/iceberg.sql
```

Або відкрити інтерактивну консоль:
```powershell
docker compose exec -it trino trino --server localhost:8080 --catalog iceberg
```

## Перевірка результатів
1. Переконатися, що продюсер надсилає повідомлення:
```bash
docker compose logs --tail=100 producer
```

2. Перевірити роботу Kafka Streams:
```bash
docker compose logs --tail=200 streams-processor
```

3. Відкрити Kafka UI: `http://localhost:8088` і перевірити, що в output-топіках з'являються повідомлення:
- `trip-avg-duration-by-day`
- `trip-count-by-day`
- `trip-top-start-station-by-day`
- `trip-top3-stations-by-day`

4. Перевірити Iceberg-таблицю через Trino:
```sql
docker compose exec -it trino trino
SELECT * FROM iceberg.db.customers;
```

## Зупинка
```bash
docker compose down
```


