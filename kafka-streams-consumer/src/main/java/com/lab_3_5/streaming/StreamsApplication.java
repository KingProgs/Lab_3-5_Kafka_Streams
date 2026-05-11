package com.lab_3_5.streaming;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.processor.TimestampExtractor;

import java.nio.charset.StandardCharsets;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class StreamsApplication {

    private static final ObjectMapper MAPPER =
        new ObjectMapper();

    private StreamsApplication() {
    }

    public static void main(String[] args) {

        String bootstrapServers = env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "broker1:9092,broker2:9092"
        );

        String appId = env(
            "STREAMS_APPLICATION_ID",
            "lab5-trip-analytics-streams"
        );

        String inputTopic = env(
            "INPUT_TOPIC",
            "Topic1"
        );

        String avgDurationTopic = env(
            "AVG_DURATION_TOPIC",
            "trip-avg-duration-by-day"
        );

        String tripCountTopic = env(
            "TRIP_COUNT_TOPIC",
            "trip-count-by-day"
        );

        String topStartStationTopic = env(
            "TOP_START_STATION_TOPIC",
            "trip-top-start-station-by-day"
        );

        String top3StationsTopic = env(
            "TOP3_STATIONS_TOPIC",
            "trip-top3-stations-by-day"
        );

        Properties props = new Properties();

        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            appId
        );

        props.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );

        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class
        );

        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            JsonSerde.class
        );

        props.put(
            StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
            3000
        );

        props.put(
            StreamsConfig.STATE_DIR_CONFIG,
            "/tmp/kafka-streams"
        );

        StreamsBuilder builder =
            new StreamsBuilder();

        buildTopology(
            builder,
            inputTopic,
            avgDurationTopic,
            tripCountTopic,
            topStartStationTopic,
            top3StationsTopic
        );

        KafkaStreams streams =
            new KafkaStreams(
                builder.build(),
                props
            );

        Runtime.getRuntime()
            .addShutdownHook(
                new Thread(streams::close)
            );

        streams.setUncaughtExceptionHandler(
            throwable -> {

                System.err.println(
                    "[streams] Uncaught error: "
                    + throwable.getMessage()
                );

                return StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse
                    .REPLACE_THREAD;
            }
        );

        streams.start();

        System.out.println(
            "Kafka Streams started"
        );
    }

    private static void buildTopology(
        StreamsBuilder builder,
        String inputTopic,
        String avgDurationTopic,
        String tripCountTopic,
        String topStartStationTopic,
        String top3StationsTopic
    ) {

        Serde<String> stringSerde =
            Serdes.String();

        JsonSerde<TripEvent> tripSerde =
            new JsonSerde<>(TripEvent.class);

        JsonSerde<DurationStats>
            durationStatsSerde =
                new JsonSerde<>(
                    DurationStats.class
                );

        JsonSerde<StationStats>
            stationStatsSerde =
                new JsonSerde<>(
                    StationStats.class
                );

        @SuppressWarnings("rawtypes")
        JsonSerde<Map> mapSerde =
            new JsonSerde<>(Map.class);

        @SuppressWarnings("rawtypes")
        JsonSerde<List> listSerde =
            new JsonSerde<>(List.class);

        TimeWindows dailyWindow =
            TimeWindows.ofSizeAndGrace(
                Duration.ofDays(1),
                Duration.ZERO
            );

        KStream<String, TripEvent> trips =
            builder
                .stream(
                    inputTopic,
                    Consumed.with(
                        stringSerde,
                        stringSerde
                    ).withTimestampExtractor(
                        new TripTimestampExtractor()
                    )
                )
                .flatMapValues(
                    StreamsApplication::parseTrip
                )
                .selectKey(
                    (key, trip) ->
                        extractDate(
                            trip.start_time
                        )
                );

        // =========================
        // AVG DURATION
        // =========================

        trips
            .groupByKey(
                Grouped.with(
                    stringSerde,
                    tripSerde
                )
            )
            .windowedBy(dailyWindow)
            .aggregate(
                DurationStats::new,
                (
                    day,
                    trip,
                    stats
                ) ->
                    stats.add(
                        trip.tripduration
                    ),
                Materialized.with(
                    stringSerde,
                    durationStatsSerde
                )
            )
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .map(
                (
                    windowedKey,
                    stats
                ) ->
                    KeyValue.pair(
                        windowedKey.key(),
                        toJson(
                            Map.of(
                                "date",
                                windowedKey.key(),
                                "average_trip_duration_seconds",
                                round2(
                                    stats.average()
                                ),
                                "trip_count",
                                stats.count
                            )
                        )
                    )
            )
            .to(
                avgDurationTopic,
                Produced.with(
                    stringSerde,
                    stringSerde
                )
            );

        // =========================
        // TRIP COUNT
        // =========================

        trips
            .groupByKey(
                Grouped.with(
                    stringSerde,
                    tripSerde
                )
            )
            .windowedBy(dailyWindow)
            .count()
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .map(
                (
                    windowedKey,
                    count
                ) ->
                    KeyValue.pair(
                        windowedKey.key(),
                        toJson(
                            Map.of(
                                "date",
                                windowedKey.key(),
                                "trip_count",
                                count
                            )
                        )
                    )
            )
            .to(
                tripCountTopic,
                Produced.with(
                    stringSerde,
                    stringSerde
                )
            );

        // =========================
        // TOP START STATION
        // =========================

        trips
            .groupByKey(
                Grouped.with(
                    stringSerde,
                    tripSerde
                )
            )
            .windowedBy(dailyWindow)
            .aggregate(
                StationStats::new,
                (
                    day,
                    trip,
                    stats
                ) ->
                    stats.addStartStation(
                        trip.from_station_name
                    ),
                Materialized.with(
                    stringSerde,
                    stationStatsSerde
                )
            )
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .map(
                (
                    windowedKey,
                    stats
                ) ->
                    KeyValue.pair(
                        windowedKey.key(),
                        toJson(
                            stats
                                .mostPopularStartStation(
                                    windowedKey.key()
                                )
                        )
                    )
            )
            .to(
                topStartStationTopic,
                Produced.with(
                    stringSerde,
                    stringSerde
                )
            );

        // =========================
        // TOP 3 STATIONS
        // =========================

        trips
            .groupByKey(
                Grouped.with(
                    stringSerde,
                    tripSerde
                )
            )
            .windowedBy(dailyWindow)
            .aggregate(
                StationStats::new,
                (
                    day,
                    trip,
                    stats
                ) ->
                    stats.addBothStations(
                        trip.from_station_name,
                        trip.to_station_name
                    ),
                Materialized.with(
                    stringSerde,
                    stationStatsSerde
                )
            )
            .suppress(
                Suppressed.untilWindowCloses(
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .map(
                (
                    windowedKey,
                    stats
                ) ->
                    KeyValue.pair(
                        windowedKey.key(),
                        toJson(
                            Map.of(
                                "date",
                                windowedKey.key(),
                                "top_3_stations",
                                stats.top3Stations()
                            )
                        )
                    )
            )
            .to(
                top3StationsTopic,
                Produced.with(
                    stringSerde,
                    stringSerde
                )
            );
    }

    // =========================
    // JSON PARSER
    // =========================

    private static List<TripEvent>
        parseTrip(
            String rawJson
        ) {

        try {

            JsonNode root =
                MAPPER.readTree(rawJson);

            JsonNode payload =
                root.path("payload");

            if (
                payload.isMissingNode()
                    || payload.isNull()
            ) {
                return List.of();
            }

            TripEvent trip =
                new TripEvent();

            trip.start_time =
                payload
                    .path("start_time")
                    .asText("");

            trip.from_station_name =
                payload
                    .path("from_station_name")
                    .asText("");

            trip.to_station_name =
                payload
                    .path("to_station_name")
                    .asText("");

            trip.tripduration =
                payload
                    .path("tripduration")
                    .asDouble(0);

            if (
                trip.start_time.isBlank()
                    || trip.from_station_name
                        .isBlank()
                    || trip.to_station_name
                        .isBlank()
            ) {
                return List.of();
            }

            return List.of(trip);

        } catch (Exception ignored) {

            return List.of();
        }
    }

    // =========================
    // TIMESTAMP EXTRACTOR
    // =========================

    static final class TripTimestampExtractor
        implements TimestampExtractor {

        @Override
        public long extract(
            ConsumerRecord<Object, Object> record,
            long partitionTime
        ) {

            Object value = record.value();

            if (value instanceof String rawJson) {

                try {

                    JsonNode root =
                        MAPPER.readTree(rawJson);

                    JsonNode payload =
                        root.path("payload");

                    String startTime =
                        payload
                            .path("start_time")
                            .asText("");

                    if (!startTime.isBlank()) {

                        return extractTimestampMillis(
                            startTime
                        );
                    }

                } catch (Exception ignored) {

                    return partitionTime;
                }
            }

            return partitionTime;
        }
    }

    // =========================
    // HELPERS
    // =========================

    private static String extractDate(
        String startTime
    ) {

        return LocalDateTime
            .parse(
                startTime.replace(" ", "T")
            )
            .toLocalDate()
            .toString();
    }

    private static long extractTimestampMillis(
        String startTime
    ) {

        return LocalDateTime
            .parse(
                startTime.replace(" ", "T")
            )
            .toInstant(ZoneOffset.UTC)
            .toEpochMilli();
    }

    private static double round2(
        double value
    ) {

        return Math.round(value * 100.0)
            / 100.0;
    }

    private static String toJson(
        Object value
    ) {

        try {

            return MAPPER.writeValueAsString(
                value
            );

        } catch (
            JsonProcessingException exception
        ) {

            throw new RuntimeException(
                exception
            );
        }
    }

    private static String env(
        String name,
        String defaultValue
    ) {

        String value =
            System.getenv(name);

        return value == null
                || value.isBlank()
            ? defaultValue
            : value;
    }

    // =========================
    // DATA CLASSES
    // =========================

    @JsonIgnoreProperties(
        ignoreUnknown = true
    )
    static final class TripEvent {

        public String start_time;

        public double tripduration;

        public String from_station_name;

        public String to_station_name;
    }

    static final class DurationStats {

        public long count = 0;

        public double totalDuration = 0.0;

        DurationStats add(
            double duration
        ) {

            count++;

            totalDuration += duration;

            return this;
        }

        double average() {

            return count == 0
                ? 0.0
                : totalDuration / count;
        }
    }

    static final class StationStats {

        public Map<String, Long>
            stations =
                new HashMap<>();

        StationStats addStartStation(
            String station
        ) {

            add(station);

            return this;
        }

        StationStats addBothStations(
            String from,
            String to
        ) {

            add(from);
            add(to);

            return this;
        }

        private void add(
            String station
        ) {

            if (
                station != null
                    && !station.isBlank()
            ) {

                stations.merge(
                    station,
                    1L,
                    Long::sum
                );
            }
        }

        Map<String, Object>
            mostPopularStartStation(
                String date
            ) {

            return stations
                .entrySet()
                .stream()
                .max(
                    Map.Entry.comparingByValue()
                )
                .map(
                    entry -> {
                        Map<String, Object> result = new HashMap<>();
                        result.put("date", date);
                        result.put("station", entry.getKey());
                        result.put("trip_count", entry.getValue());
                        return result;
                    }
                )
                .orElseGet(() -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("date", date);
                    result.put("station", "");
                    result.put("trip_count", 0);
                    return result;
                });
        }

        List<Map<String, Object>>
            top3Stations() {

            return stations
                .entrySet()
                .stream()
                .sorted(
                    Map.Entry
                        .<String, Long>
                            comparingByValue()
                        .reversed()
                )
                .limit(3)
                .map(
                    entry -> {
                        Map<String, Object> result = new HashMap<>();
                        result.put("station", entry.getKey());
                        result.put("trip_count", entry.getValue());
                        return result;
                    }
                )
                .toList();
        }
    }

    // =========================
    // JSON SERDE
    // =========================

    static final class JsonSerde<T>
        implements Serde<T> {

        private final ObjectMapper mapper =
            new ObjectMapper();

        private final Class<T> type;

        JsonSerde(
            Class<T> type
        ) {

            this.type = type;
        }

        @Override
        public Serializer<T> serializer() {

            return (topic, data) -> {

                try {

                    return mapper
                        .writeValueAsBytes(
                            data
                        );

                } catch (Exception exception) {

                    throw new RuntimeException(
                        exception
                    );
                }
            };
        }

        @Override
        public Deserializer<T>
            deserializer() {

            return (topic, data) -> {

                try {

                    if (
                        data == null
                            || type == null
                    ) {
                        return null;
                    }

                    return mapper.readValue(
                        new String(
                            data,
                            StandardCharsets.UTF_8
                        ),
                        type
                    );

                } catch (Exception exception) {

                    throw new RuntimeException(
                        exception
                    );
                }
            };
        }
    }
}