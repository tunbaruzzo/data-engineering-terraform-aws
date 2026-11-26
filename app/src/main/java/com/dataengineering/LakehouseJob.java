package com.dataengineering;

// ============================================================
// DATA ENGINEERING - PRE-ENTREGA 05
// KINESIS -> FLINK -> ICEBERG
// JDBC CATALOG + POSTGRESQL + LOCALSTACK
// ============================================================

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

// Apache Flink
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// Kinesis
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;

// JSON
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

// Tipos internos de Flink
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;

// Apache Iceberg
import org.apache.iceberg.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

public class LakehouseJob {

    // ========================================================
    // CONFIGURACION GENERAL
    // ========================================================

    private static final String REGION = "us-east-1";

    private static final String ENDPOINT =
            "http://localstack:4566";

    private static final String WAREHOUSE =
            "s3://lakehouse/warehouse";

    private static final String CATALOG_NAME =
            "jdbc_catalog";

    private static final String DATABASE =
            "lakehouse_db";

    private static final String TABLE =
            "transaction_metrics";

    private static final long CHECKPOINT_MS = 60000L;

    private static final int WINDOW_SECONDS = 60;

    private static final int WATERMARK_SECONDS = 10;

    // ========================================================
    // CONFIGURACION DE CREDENCIALES
    // ========================================================

    private static String requiredEnv(String name) {

        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Variable de entorno requerida: " + name
            );
        }

        return value;
    }

    // ========================================================
    // CATALOGO JDBC DE ICEBERG
    // ========================================================

    private static CatalogLoader createCatalogLoader() {

        Map<String, String> properties = new HashMap<>();

        properties.put(
                "uri",
                "jdbc:postgresql://postgres:5432/lakehouse"
        );

        properties.put(
                "jdbc.user",
                requiredEnv("POSTGRES_USER")
        );

        properties.put(
                "jdbc.password",
                requiredEnv("POSTGRES_PASSWORD")
        );

        properties.put("warehouse", WAREHOUSE);

        properties.put(
                "io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO"
        );

        properties.put("s3.endpoint", ENDPOINT);

        properties.put("s3.path-style-access", "true");

        properties.put("client.region", REGION);

        // Credenciales ficticias exclusivas de LocalStack.
        properties.put("s3.access-key-id", "test");
        properties.put("s3.secret-access-key", "test");

        return CatalogLoader.custom(
                CATALOG_NAME,
                properties,
                new org.apache.hadoop.conf.Configuration(false),
                "org.apache.iceberg.jdbc.JdbcCatalog"
        );
    }

    // ========================================================
    // DEFINICION DE LA TABLA ICEBERG
    // ========================================================

    private static void createIcebergTable(
            CatalogLoader catalogLoader,
            TableIdentifier identifier) {

        Catalog catalog = catalogLoader.loadCatalog();

        SupportsNamespaces namespaces =
                (SupportsNamespaces) catalog;

        Namespace namespace = Namespace.of(DATABASE);

        // Crear el namespace si todavia no existe.
        if (!namespaces.namespaceExists(namespace)) {

            namespaces.createNamespace(namespace);
        }

        // Conservar los datos si la tabla ya existe.
        if (catalog.tableExists(identifier)) {

            return;
        }

        // Mismo esquema utilizado en AWS.
        Schema schema = new Schema(

                Types.NestedField.required(
                        1, "window_start",
                        Types.TimestampType.withoutZone()
                ),

                Types.NestedField.required(
                        2, "window_end",
                        Types.TimestampType.withoutZone()
                ),

                Types.NestedField.required(
                        3, "event_type",
                        Types.StringType.get()
                ),

                Types.NestedField.required(
                        4, "transaction_count",
                        Types.LongType.get()
                ),

                Types.NestedField.optional(
                        5, "total_amount",
                        Types.DoubleType.get()
                ),

                Types.NestedField.optional(
                        6, "average_amount",
                        Types.DoubleType.get()
                )
        );

        // Particionado diario derivado de window_start.
        PartitionSpec partitionSpec =
                PartitionSpec.builderFor(schema)
                        .day("window_start")
                        .build();

        Map<String, String> tableProperties =
                new HashMap<>();

        tableProperties.put("format-version", "2");

        tableProperties.put(
                "write.format.default",
                "parquet"
        );

        catalog.createTable(
                identifier,
                schema,
                partitionSpec,
                tableProperties
        );
    }

    // ========================================================
    // MODELO DE EVENTO
    // ========================================================

    public static class TransactionEvent {

        public String eventType;
        public Double amount;
        public long timestamp;

        public TransactionEvent() {
        }

        public TransactionEvent(
                String eventType,
                Double amount,
                long timestamp) {

            this.eventType = eventType;
            this.amount = amount;
            this.timestamp = timestamp;
        }
    }

    // ========================================================
    // DESERIALIZACION DE EVENTOS JSON
    // ========================================================

    public static class ParseEvent
            implements MapFunction<String, TransactionEvent> {

        private transient ObjectMapper mapper;

        @Override
        public TransactionEvent map(String value)
                throws Exception {

            if (mapper == null) {

                mapper = new ObjectMapper();
            }

            JsonNode json = mapper.readTree(value);

            String eventType =
                    json.path("event_type").asText(null);

            JsonNode amountNode = json.get("amount");

            Double amount =
                    amountNode == null || amountNode.isNull()
                            ? null
                            : amountNode.asDouble();

            long timestamp = OffsetDateTime.parse(
                    json.get("timestamp").asText()
            ).toInstant().toEpochMilli();

            return new TransactionEvent(
                    eventType,
                    amount,
                    timestamp
            );
        }
    }

    // ========================================================
    // ESTADO DE AGREGACION
    // ========================================================

    public static class MetricsAccumulator {

        public long count;
        public long amountCount;
        public double total;

        public MetricsAccumulator() {
        }
    }

    // ========================================================
    // AGREGACION INCREMENTAL POR VENTANA
    // ========================================================

    public static class MetricsAggregate implements
            AggregateFunction<
                    TransactionEvent,
                    MetricsAccumulator,
                    MetricsAccumulator> {

        @Override
        public MetricsAccumulator createAccumulator() {

            return new MetricsAccumulator();
        }

        @Override
        public MetricsAccumulator add(
                TransactionEvent event,
                MetricsAccumulator accumulator) {

            accumulator.count++;

            if (event.amount != null) {

                accumulator.total += event.amount;
                accumulator.amountCount++;
            }

            return accumulator;
        }

        @Override
        public MetricsAccumulator getResult(
                MetricsAccumulator accumulator) {

            return accumulator;
        }

        @Override
        public MetricsAccumulator merge(
                MetricsAccumulator first,
                MetricsAccumulator second) {

            first.count += second.count;

            first.amountCount += second.amountCount;

            first.total += second.total;

            return first;
        }
    }

    // ========================================================
    // CONVERSION AL ESQUEMA DE SALIDA ICEBERG
    // ========================================================

    public static class WindowMetrics extends
            ProcessWindowFunction<
                    MetricsAccumulator,
                    RowData,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String eventType,
                Context context,
                Iterable<MetricsAccumulator> elements,
                Collector<RowData> output) {

            MetricsAccumulator metrics =
                    elements.iterator().next();

            Double total = metrics.amountCount == 0
                    ? null
                    : metrics.total;

            Double average = metrics.amountCount == 0
                    ? null
                    : metrics.total / metrics.amountCount;

            GenericRowData row = GenericRowData.of(

                    TimestampData.fromEpochMillis(
                            context.window().getStart()
                    ),

                    TimestampData.fromEpochMillis(
                            context.window().getEnd()
                    ),

                    StringData.fromString(eventType),

                    metrics.count,

                    total,

                    average
            );

            output.collect(row);
        }
    }

    // ========================================================
    // TIPO DE FILA PARA FLINK E ICEBERG
    // ========================================================

    private static InternalTypeInfo<RowData> outputType() {

        return InternalTypeInfo.of(

                RowType.of(

                        new LogicalType[] {

                                new TimestampType(6),
                                new TimestampType(6),
                                new VarCharType(),
                                new BigIntType(),
                                new DoubleType(),
                                new DoubleType()
                        },

                        new String[] {

                                "window_start",
                                "window_end",
                                "event_type",
                                "transaction_count",
                                "total_amount",
                                "average_amount"
                        }
                )
        );
    }

    // ========================================================
    // APLICACION PRINCIPAL
    // ========================================================

    public static void main(String[] args)
            throws Exception {

        // Inicializar el driver JDBC.
        Class.forName("org.postgresql.Driver");

        if (args.length != 1) {

            throw new IllegalArgumentException(
                    "Se requiere el ARN del stream Kinesis local"
            );
        }

        String streamArn = args[0];

        // ====================================================
        // ENTORNO DE EJECUCION Y CHECKPOINTING
        // ====================================================

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        env.setParallelism(1);

        // Iceberg confirma los archivos mediante checkpoints.
        env.enableCheckpointing(
                CHECKPOINT_MS,
                CheckpointingMode.EXACTLY_ONCE
        );

        env.getCheckpointConfig()
                .setCheckpointTimeout(120000L);

        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(1);

        // ====================================================
        // INICIALIZAR CATALOGO Y TABLA
        // ====================================================

        CatalogLoader catalogLoader =
                createCatalogLoader();

        TableIdentifier identifier =
                TableIdentifier.of(DATABASE, TABLE);

        createIcebergTable(
                catalogLoader,
                identifier
        );

        TableLoader tableLoader =
                TableLoader.fromCatalog(
                        catalogLoader,
                        identifier
                );

        // ====================================================
        // FUENTE KINESIS - LOCALSTACK
        // ====================================================

        Configuration sourceConfig =
                new Configuration();

        sourceConfig.setString("aws.region", REGION);

        sourceConfig.setString("aws.endpoint", ENDPOINT);

        sourceConfig.setString(
                "aws.credentials.provider",
                "ENV_VAR"
        );

        sourceConfig.set(
                KinesisSourceConfigOptions.STREAM_INITIAL_POSITION,
                KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON
        );

        KinesisStreamsSource<String> source =
                KinesisStreamsSource.<String>builder()
                        .setStreamArn(streamArn)
                        .setSourceConfig(sourceConfig)
                        .setDeserializationSchema(
                                new SimpleStringSchema()
                        )
                        .build();

        // ====================================================
        // PROCESAMIENTO DE EVENTOS
        // ====================================================

        DataStream<TransactionEvent> events = env

                .fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kinesis LocalStack",
                        org.apache.flink.api.common.typeinfo.Types.STRING
                )

                .uid("kinesis-source")

                .map(new ParseEvent())

                .name("Parse JSON")

                .uid("parse-json")

                .assignTimestampsAndWatermarks(

                        WatermarkStrategy
                                .<TransactionEvent>
                                forBoundedOutOfOrderness(
                                        Duration.ofSeconds(
                                                WATERMARK_SECONDS
                                        )
                                )

                                .withTimestampAssigner(
                                        (event, timestamp) ->
                                                event.timestamp
                                )

                                .withIdleness(
                                        Duration.ofSeconds(30)
                                )
                );

        // ====================================================
        // AGREGACION STATEFUL
        // ====================================================

        DataStream<RowData> metrics = events

                .filter(event ->
                        event.eventType != null
                )

                .keyBy(event -> event.eventType)

                .window(
                        TumblingEventTimeWindows.of(
                                Time.seconds(WINDOW_SECONDS)
                        )
                )

                .aggregate(
                        new MetricsAggregate(),
                        new WindowMetrics()
                )

                .returns(outputType())

                .name("Transaction metrics")

                .uid("transaction-metrics");

        // ====================================================
        // ICEBERG SINK
        // API JAVA SOLICITADA EN LA PRE-ENTREGA 05
        // ====================================================

        IcebergSink.forRowData(metrics)

                .tableLoader(tableLoader)

                .writeParallelism(1)

                .append();

        // ====================================================
        // EJECUTAR PIPELINE
        // ====================================================

        env.execute(
                "Preentrega 05 - Local Iceberg Lakehouse"
        );
    }
}