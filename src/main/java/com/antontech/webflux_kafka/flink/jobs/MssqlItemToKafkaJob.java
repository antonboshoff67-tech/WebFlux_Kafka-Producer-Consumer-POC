package com.antontech.webflux_kafka.flink.jobs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.antontech.webflux_kafka.kafka.consumer.LocalDateTimeAdapter;
import com.antontech.webflux_kafka.model.Item;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink batch job: Source = MS SQL Server ({@code ITEM} master-data table),
 * Sink = Kafka topic.
 * <p>
 * This is the "active"/current version of the original POC job (previously
 * named {@code FlinkJob1}). It reads a batch of rows from the configured MS
 * SQL Server table, converts each row to an {@link Item}, serializes it to
 * JSON with Gson and publishes it onto the configured Kafka topic.
 * <p>
 * All connection details (JDBC URL, Kafka bootstrap servers, topic name,
 * source table name) are supplied by the caller - see
 * {@link com.antontech.itemkafka_poc.service.FlinkJobService#runJob1()} which
 * wires these values in from {@code application.yml} /
 * environment variables. Nothing is hardcoded here anymore.
 * <p>
 * Kick off via {@code POST /flink/start-job1}.
 */
@Slf4j
public class MssqlItemToKafkaJob
{
    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .serializeNulls()
            .create();

    private static final int MAX_ROWS_PER_RUN = 100;

    private final String jdbcUrl;
    private final String bootstrapServers;
    private final String topicName;
    private final String sourceTableName;

    /**
     * Creates a new job instance.
     *
     * @param jdbcUrl          MS SQL Server JDBC connection string (see {@code spring.datasource.url}).
     * @param bootstrapServers Kafka bootstrap servers, e.g. {@code localhost:9092}.
     * @param topicName        Kafka topic to publish Item JSON messages to.
     * @param sourceTableName  name of the source table to read Item rows from, e.g. {@code ITEM}.
     */
    public MssqlItemToKafkaJob(String jdbcUrl, String bootstrapServers, String topicName, String sourceTableName)
    {
        this.jdbcUrl = jdbcUrl;
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.sourceTableName = sourceTableName;
    }

    /**
     * Executes the job synchronously: reads up to {@value #MAX_ROWS_PER_RUN}
     * rows from the source table, then submits a small Flink DataStream
     * pipeline that publishes each row as a JSON message to Kafka.
     * <p>
     * Any JDBC failure aborts the run before Flink is invoked; any Flink
     * execution failure is logged and swallowed so that the calling REST
     * endpoint can still return a response.
     */
    public void run()
    {
        log.debug("Starting MssqlItemToKafkaJob to connect to SQL Server and publish to Kafka.");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String query = "SELECT TOP " + MAX_ROWS_PER_RUN + " * FROM " + sourceTableName;

        List<Item> items = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query))
        {
            while (resultSet.next())
            {
                items.add(createItemFromResultSet(resultSet));
            }
            log.debug("Fetched {} items from SQL Server table {}.", items.size(), sourceTableName);
        }
        catch (Exception e)
        {
            log.error("Error fetching data from SQL Server: {}", e.getMessage());
            return;
        }

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        try
        {
            DataStream<String> messageStream = env.fromData(items)
                    .map(new ItemToJsonFunction())
                    .map(itemJson ->
                    {
                        log.debug("Publishing Item JSON to Kafka: {}", itemJson);
                        return itemJson;
                    });

            messageStream.sinkTo(kafkaSink);

            env.execute("MssqlItemToKafkaJob: Read from SQL Server and Publish to Kafka");
            log.debug("MssqlItemToKafkaJob executed successfully.");
        }
        catch (Exception e)
        {
            log.error("Execution failed for MssqlItemToKafkaJob: {}", e.getMessage());
        }
    }

    /**
     * Maps the current row of the given {@link ResultSet} onto a new {@link Item}.
     *
     * @param rs an open result set positioned on the row to map.
     * @return a fully populated {@link Item} instance.
     * @throws Exception if any column cannot be read (e.g. driver/type mismatch).
     */
    private static Item createItemFromResultSet(ResultSet rs) throws Exception
    {
        Item item = new Item();
        item.setItemId(rs.getString("item_id"));
        item.setItemLevel(rs.getInt("item_level"));
        item.setItemNumberType(rs.getString("item_number_type"));
        item.setPrefix(rs.getInt("prefix"));
        item.setAllocatorSystem(rs.getString("allocator_system"));
        item.setBusinessUnitId(rs.getInt("business_unit_id"));
        item.setCatchWeightInd(rs.getString("catch_weight_ind"));
        item.setClassId(rs.getInt("class_id"));
        item.setColourDsc(rs.getString("colour_dsc"));
        item.setColourGroupId(rs.getString("colour_group_id"));
        item.setColourId(rs.getString("colour_id"));
        item.setColourRangeId(rs.getInt("colour_range_id"));
        item.setCompanyId(rs.getInt("company_id"));
        item.setCountOnUsId(rs.getString("count_on_us_id"));

        Timestamp createTimestamp = rs.getTimestamp("create_dte");
        if (createTimestamp != null)
        {
            item.setCreateDte(createTimestamp.toLocalDateTime());
        }

        item.setDeptId(rs.getInt("dept_id"));
        item.setDiscipline(rs.getString("discipline"));
        item.setDomainId(rs.getInt("domain_id"));
        item.setFlavourDsc(rs.getString("flavour_dsc"));
        item.setFlavourGroupId(rs.getString("flavour_group_id"));
        item.setFlavourId(rs.getString("flavour_id"));
        item.setFlavourRangeId(rs.getInt("flavour_range_id"));
        item.setForecastInd(rs.getString("forecast_ind"));
        item.setFreeRangeId(rs.getString("free_range_id"));
        item.setFromTemp(rs.getInt("from_temp"));
        item.setGroupId(rs.getInt("group_id"));
        item.setHighMaxTemp(rs.getInt("high_max_temp"));
        item.setHighMinTemp(rs.getInt("high_min_temp"));
        item.setItemGrandparent(rs.getString("item_grandparent"));
        item.setItemParent(rs.getString("item_parent"));
        item.setKidzId(rs.getString("kidz_id"));
        item.setOrderableInd(rs.getString("orderable_ind"));
        item.setPackInd(rs.getString("pack_ind"));
        item.setPackMember(rs.getString("pack_member"));
        item.setPackQty(rs.getBigDecimal("pack_qty"));
        item.setPhaseId(rs.getInt("phase_id"));
        item.setPriceMarkInd(rs.getString("price_mark_ind"));
        item.setPrimaryRefItemInd(rs.getString("primary_ref_item_ind"));
        item.setPrimarySizeDsc(rs.getString("primary_size_dsc"));
        item.setPrimarySizeGroupId(rs.getString("primary_size_group_id"));
        item.setPrimarySizeId(rs.getString("primary_size_id"));
        item.setPrimarySizeRangeId(rs.getInt("primary_size_range_id"));
        item.setProductGroupScaling(rs.getString("product_group_scaling"));
        item.setProductId(rs.getString("product_id"));
        item.setReferenceItemInd(rs.getString("reference_item_ind"));
        item.setScentDsc(rs.getString("scent_dsc"));
        item.setScentGroupId(rs.getString("scent_group_id"));
        item.setScentId(rs.getString("scent_id"));
        item.setScentRangeId(rs.getInt("scent_range_id"));
        item.setSeasonId(rs.getInt("season_id"));
        item.setSecondarySizeDsc(rs.getString("secondary_size_dsc"));
        item.setSecondarySizeGroupId(rs.getString("secondary_size_group_id"));
        item.setSecondarySizeId(rs.getString("secondary_size_id"));
        item.setSecondarySizeRangeId(rs.getInt("secondary_size_range_id"));
        item.setSellableInd(rs.getString("sellable_ind"));
        item.setShortDsc(rs.getString("short_dsc"));
        item.setSimplePackInd(rs.getString("simple_pack_ind"));
        item.setSizeProfileInd(rs.getString("size_profile_ind"));
        item.setStandardUom(rs.getString("standard_uom"));
        item.setStatus(rs.getString("status"));
        item.setSubGroupId(rs.getInt("sub_group_id"));
        item.setSubclassId(rs.getInt("subclass_id"));
        item.setSupplierNo(rs.getInt("supplier_no"));
        item.setToTemp(rs.getInt("to_temp"));
        item.setTranInd(rs.getString("tran_ind"));
        item.setTranLevel(rs.getInt("tran_level"));
        item.setWwColour(rs.getString("ww_colour"));
        item.setWwSize(rs.getString("ww_size"));
        item.setWwStaticMass(rs.getBigDecimal("ww_static_mass"));
        item.setWwStyle(rs.getString("ww_style"));
        item.setWwStyleColour(rs.getString("ww_style_colour"));

        item.setVariableWeightInd(rs.getString("variable_weight_ind") != null ? rs.getString("variable_weight_ind").charAt(0) : null);
        item.setLooseProdInd(rs.getString("loose_prod_ind") != null ? rs.getString("loose_prod_ind").charAt(0) : null);
        item.setItemScaleInd(rs.getString("item_scale_ind") != null ? rs.getString("item_scale_ind").charAt(0) : null);
        item.setLegacySkuNo(rs.getString("legacy_sku_no"));
        item.setLegacyRandomMassInd(rs.getString("legacy_random_mass_ind") != null ? rs.getString("legacy_random_mass_ind").charAt(0) : null);
        item.setLegacyVatInd(rs.getString("legacy_vat_ind") != null ? rs.getString("legacy_vat_ind").charAt(0) : null);
        item.setActionInd(rs.getString("action_ind") != null ? rs.getString("action_ind").charAt(0) : null);

        item.setExtractSeqNo(rs.getLong("extract_seq_no"));
        item.setVatCde(rs.getString("vat_cde"));
        item.setVatRate(rs.getBigDecimal("vat_rate"));
        item.setSourceSystem(rs.getString("source_system"));
        item.setVpnNo(rs.getString("vpn_no"));
        item.setExtRefNo(rs.getString("ext_ref_no"));
        item.setItemLongDesc(rs.getString("item_long_desc"));
        item.setSegregationInd(rs.getString("segregation_ind"));
        item.setProdClass(rs.getString("prod_class"));

        Timestamp lastUpdateTimestamp = rs.getTimestamp("last_update_dte");
        if (lastUpdateTimestamp != null)
        {
            item.setLastUpdateDte(lastUpdateTimestamp.toLocalDateTime());
        }

        log.debug("Creating Item for ItemId: {}", item.getItemId());
        return item;
    }

    /** Serializable Flink {@link MapFunction} that converts an {@link Item} into its JSON representation. */
    public static class ItemToJsonFunction implements MapFunction<Item, String>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;

        /**
         * @param item the item to serialize.
         * @return the JSON representation of {@code item}.
         */
        @Override
        public String map(Item item)
        {
            return gson.toJson(item);
        }
    }
}

