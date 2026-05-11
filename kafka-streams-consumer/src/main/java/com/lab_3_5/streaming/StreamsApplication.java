package com.lab_3_5.streaming;

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

import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.ValueJoiner;

import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class StreamsApplication {

    private static final ObjectMapper MAPPER =
        new ObjectMapper();

    private static final String DAY_STATION_DELIMITER =
        "\u001F";

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
            Serdes.String().getClass().getName()
        );

        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName()
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
            new KafkaStreams(builder.build(), props);

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

        Serde<Long> longSerde =
            Serdes.Long();

        JsonSerde<StationCount> stationCountSerde =
            new JsonSerde<>(StationCount.class);

        JsonSerde<TopStationsState>
            topStationsStateSerde =
                new JsonSerde<>(
                    TopStationsState.class
                );

        // =========================
        // INPUT STREAM
        // =========================

        KStream<String, TripRecord> trips =
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
                    StreamsApplication::parseTripRecord
                );

        // =========================
        // TRIP COUNT BY DAY
        // =========================

        KTable<String, Long> tripCountByDay =
            trips
                .map(
                    (key, trip) ->
                        KeyValue.pair(
                            trip.tripDay,
                            1L
                        )
                )
                .groupByKey(
                    Grouped.with(
                        stringSerde,
                        longSerde
                    )
                )
                .count(
                    Materialized.as(
                        "trip-count-by-day-store"
                    )
                );

        // =========================
        // DURATION SUM BY DAY
        // =========================

        KTable<String, Long> durationSumByDay =
            trips
                .map(
                    (key, trip) ->
                        KeyValue.pair(
                            trip.tripDay,
                            trip.durationSeconds
                        )
                )
                .groupByKey(
                    Grouped.with(
                        stringSerde,
                        longSerde
                    )
                )
                .reduce(
                    Long::sum,
                    Materialized.as(
                        "duration-sum-by-day-store"
                    )
                );

        // =========================
        // AVERAGE DURATION
        // =========================

        ValueJoiner<Long, Long, Double>
            averageDurationJoiner =
                (sumSeconds, tripCount) ->
                    tripCount == 0
                        ? 0.0
                        : (double) sumSeconds
                            / tripCount;

        KTable<String, Double> avgDurationByDay =
            durationSumByDay.join(
                tripCountByDay,
                averageDurationJoiner
            );

        avgDurationByDay
            .suppress(
                Suppressed.untilTimeLimit(
                    Duration.ofSeconds(5),
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .mapValues(
                (day, avgSeconds) ->
                    toJson(
                        Map.of(
                            "date",
                            day,
                            "average_trip_duration_seconds",
                            round2(avgSeconds)
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
        // TRIP COUNT OUTPUT
        // =========================

        tripCountByDay
            .suppress(
                Suppressed.untilTimeLimit(
                    Duration.ofSeconds(5),
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .mapValues(
                (day, count) ->
                    toJson(
                        Map.of(
                            "date",
                            day,
                            "trip_count",
                            count
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

        KTable<String, Long>
            startStationCountByDay =
                trips
                    .map(
                        (key, trip) ->
                            KeyValue.pair(
                                compositeDayStationKey(
                                    trip.tripDay,
                                    trip.fromStation
                                ),
                                1L
                            )
                    )
                    .groupByKey(
                        Grouped.with(
                            stringSerde,
                            longSerde
                        )
                    )
                    .count(
                        Materialized.as(
                            "start-station-count-by-day-store"
                        )
                    );

        KTable<String, StationCount>
            topStartStationByDay =
                startStationCountByDay
                    .toStream()
                    .map(
                        (dayStationKey, count) -> {

                            DayStation dayStation =
                                splitCompositeDayStationKey(
                                    dayStationKey
                                );

                            return KeyValue.pair(
                                dayStation.day,
                                new StationCount(
                                    dayStation.station,
                                    count
                                )
                            );
                        }
                    )
                    .groupByKey(
                        Grouped.with(
                            stringSerde,
                            stationCountSerde
                        )
                    )
                    .reduce(
                        StreamsApplication
                            ::maxStationCount,
                        Materialized.as(
                            "top-start-station-by-day-store"
                        )
                    );

        topStartStationByDay
            .suppress(
                Suppressed.untilTimeLimit(
                    Duration.ofSeconds(5),
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .mapValues(
                (day, stationCount) ->
                    toJson(
                        Map.of(
                            "date",
                            day,
                            "station",
                            stationCount.station,
                            "trip_count",
                            stationCount.count
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

        KStream<String, StationCount>
            stationCountsByDayUpdates =
                trips
                    .flatMap(
                        (key, trip) ->
                            List.of(
                                KeyValue.pair(
                                    compositeDayStationKey(
                                        trip.tripDay,
                                        trip.fromStation
                                    ),
                                    1L
                                ),
                                KeyValue.pair(
                                    compositeDayStationKey(
                                        trip.tripDay,
                                        trip.toStation
                                    ),
                                    1L
                                )
                            )
                    )
                    .groupByKey(
                        Grouped.with(
                            stringSerde,
                            longSerde
                        )
                    )
                    .count(
                        Materialized.as(
                            "all-station-count-by-day-store"
                        )
                    )
                    .toStream()
                    .map(
                        (dayStationKey, count) -> {

                            DayStation dayStation =
                                splitCompositeDayStationKey(
                                    dayStationKey
                                );

                            return KeyValue.pair(
                                dayStation.day,
                                new StationCount(
                                    dayStation.station,
                                    count
                                )
                            );
                        }
                    );

        KTable<String, TopStationsState>
            top3StationsByDay =
                stationCountsByDayUpdates
                    .groupByKey(
                        Grouped.with(
                            stringSerde,
                            stationCountSerde
                        )
                    )
                    .aggregate(
                        TopStationsState::new,
                        (
                            day,
                            stationCount,
                            aggregate
                        ) ->
                            aggregate
                                .withStationCount(
                                    stationCount.station,
                                    stationCount.count
                                ),
                        Materialized.with(
                            stringSerde,
                            topStationsStateSerde
                        )
                    );

        top3StationsByDay
            .suppress(
                Suppressed.untilTimeLimit(
                    Duration.ofSeconds(5),
                    Suppressed.BufferConfig
                        .unbounded()
                )
            )
            .toStream()
            .mapValues(
                (day, state) ->
                    toJson(
                        Map.of(
                            "date",
                            day,
                            "top_3_stations",
                            state.top3AsList()
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
    // PARSE INPUT JSON
    // =========================

    private static List<TripRecord>
        parseTripRecord(
            String rawEventJson
        ) {

        try {

            JsonNode root =
                MAPPER.readTree(rawEventJson);

            JsonNode payload =
                root.path("payload");

            if (
                payload.isMissingNode()
                    || payload.isNull()
            ) {
                return List.of();
            }

            String startTime =
                payload
                    .path("start_time")
                    .asText("")
                    .trim();

            String fromStation =
                payload
                    .path("from_station_name")
                    .asText("")
                    .trim();

            String toStation =
                payload
                    .path("to_station_name")
                    .asText("")
                    .trim();

            double tripDuration =
                payload
                    .path("tripduration")
                    .asDouble(-1.0);

            if (
                startTime.length() < 10
                    || fromStation.isEmpty()
                    || toStation.isEmpty()
                    || tripDuration < 0
            ) {
                return List.of();
            }

            String day =
                startTime.substring(0, 10);

            long durationSeconds =
                Math.round(tripDuration);

            return List.of(
                new TripRecord(
                    day,
                    durationSeconds,
                    fromStation,
                    toStation
                )
            );

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
                            .asText("")
                            .trim();

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

    private static String compositeDayStationKey(
        String day,
        String station
    ) {

        return day
            + DAY_STATION_DELIMITER
            + station;
    }

    private static DayStation
        splitCompositeDayStationKey(
            String value
        ) {

        int separator =
            value.indexOf(
                DAY_STATION_DELIMITER
            );

        if (separator < 0) {

            return new DayStation(
                value,
                ""
            );
        }

        String day =
            value.substring(0, separator);

        String station =
            value.substring(
                separator
                    + DAY_STATION_DELIMITER.length()
            );

        return new DayStation(
            day,
            station
        );
    }

    private static StationCount maxStationCount(
        StationCount current,
        StationCount incoming
    ) {

        if (incoming.count > current.count) {
            return incoming;
        }

        if (
            incoming.count == current.count
                && incoming.station.compareTo(
                    current.station
                ) < 0
        ) {
            return incoming;
        }

        return current;
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
                "Could not serialize JSON",
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

    static final class TripRecord {

        final String tripDay;
        final long durationSeconds;
        final String fromStation;
        final String toStation;

        TripRecord(
            String tripDay,
            long durationSeconds,
            String fromStation,
            String toStation
        ) {

            this.tripDay = tripDay;
            this.durationSeconds =
                durationSeconds;
            this.fromStation =
                fromStation;
            this.toStation =
                toStation;
        }
    }

    static final class DayStation {

        final String day;
        final String station;

        DayStation(
            String day,
            String station
        ) {

            this.day = day;
            this.station = station;
        }
    }

    static final class StationCount {

        public String station;
        public long count;

        public StationCount() {
        }

        StationCount(
            String station,
            long count
        ) {

            this.station = station;
            this.count = count;
        }
    }

    static final class TopStationsState {

        public Map<String, Long>
            stationCounts =
                new HashMap<>();

        public TopStationsState() {
        }

        TopStationsState withStationCount(
            String station,
            long count
        ) {

            stationCounts.put(
                station,
                count
            );

            return this;
        }

        List<Map<String, Object>>
            top3AsList() {

            return stationCounts
                .entrySet()
                .stream()
                .sorted(
                    Comparator
                        .comparingLong(
                            (
                                Map.Entry<
                                    String,
                                    Long
                                > entry
                            ) ->
                                entry.getValue()
                        )
                        .reversed()
                        .thenComparing(
                            Map.Entry::getKey
                        )
                )
                .limit(3)
                .collect(
                    ArrayList::new,
                    (list, entry) ->
                        list.add(
                            Map.of(
                                "station",
                                entry.getKey(),
                                "trip_count",
                                entry.getValue()
                            )
                        ),
                    ArrayList::addAll
                );
        }
    }

    static final class JsonSerde<T>
        implements Serde<T> {

        private final ObjectMapper mapper =
            new ObjectMapper();

        private final Class<T> clazz;

        JsonSerde(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public Serializer<T> serializer() {

            return (topic, data) -> {

                if (data == null) {
                    return null;
                }

                try {

                    return mapper
                        .writeValueAsBytes(data);

                } catch (Exception exception) {

                    throw new RuntimeException(
                        "Could not serialize "
                            + clazz.getSimpleName(),
                        exception
                    );
                }
            };
        }

        @Override
        public Deserializer<T> deserializer() {

            return (topic, data) -> {

                if (data == null) {
                    return null;
                }

                try {

                    return mapper.readValue(
                        data,
                        clazz
                    );

                } catch (Exception exception) {

                    throw new RuntimeException(
                        "Could not deserialize "
                            + clazz.getSimpleName(),
                        exception
                    );
                }
            };
        }
    }
}