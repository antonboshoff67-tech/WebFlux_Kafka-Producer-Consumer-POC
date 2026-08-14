package com.antontech.webflux_kafka.flink.jobs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.antontech.webflux_kafka.kafka.consumer.LocalDateTimeAdapter;
import com.antontech.webflux_kafka.model.Item;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Flink streaming job: Source = Kafka topic (Item JSON messages), Sink =
 * MySQL {@code ITEM} table (upsert via {@code ON DUPLICATE KEY UPDATE}).
 * <p>
 * This is the "active" version of the original POC job (previously named
 * {@code FlinkJob2_cmdLine}; the older hardcoded {@code FlinkJob2} variant
 * has been removed as it duplicated this logic without externalised
 * configuration). It continuously consumes messages from the configured
 * Kafka topic, deserializes them into {@link Item} objects (defaulting any
 * missing/null fields so that a partial upstream payload doesn't break the
 * insert), and writes them to MySQL in batches.
 * <p>
 * All connection details (Kafka bootstrap servers/topic/group, MySQL JDBC
 * URL/credentials/table name) are supplied by the caller - see
 * {@link com.antontech.itemkafka_poc.service.FlinkJobService#runJob2()} which
 * wires these values in from {@code application.yml} / environment
 * variables. Nothing is hardcoded here anymore.
 * <p>
 * Kick off via {@code POST /flink/start-job2}. Note that unlike Job 1, this
 * job is a genuine unbounded stream and will keep running until the process
 * exits or the Flink job is cancelled.
 */
@Slf4j
public class KafkaItemToMysqlJob
{
    private final String bootstrapServers;
    private final String topicName;
    private final String groupId;
    private final String mysqlJdbcUrl;
    private final String mysqlUsername;
    private final String mysqlPassword;
    private final String targetTableName;

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .serializeNulls()
            .create();

    /**
     * Creates a new job instance.
     *
     * @param bootstrapServers Kafka bootstrap servers, e.g. {@code localhost:9092}.
     * @param topicName        Kafka topic to consume Item JSON messages from.
     * @param groupId          Kafka consumer group id for this job.
     * @param mysqlJdbcUrl     MySQL JDBC connection string (see {@code spring.mysql.jdbcUrl}).
     * @param mysqlUsername    MySQL username.
     * @param mysqlPassword    MySQL password.
     * @param targetTableName  destination table name to upsert Item rows into, e.g. {@code ITEM}.
     */
    public KafkaItemToMysqlJob(String bootstrapServers, String topicName, String groupId,
                               String mysqlJdbcUrl, String mysqlUsername, String mysqlPassword,
                               String targetTableName)
    {
        this.bootstrapServers = bootstrapServers;
        this.topicName = topicName;
        this.groupId = groupId;
        this.mysqlJdbcUrl = mysqlJdbcUrl;
        this.mysqlUsername = mysqlUsername;
        this.mysqlPassword = mysqlPassword;
        this.targetTableName = targetTableName;
    }

    private String buildInsertQuery()
    {
        return "INSERT INTO " + targetTableName + " (item_id, item_level, item_number_type, prefix, allocator_system, " +
                "business_unit_id, catch_weight_ind, class_id, colour_dsc, colour_group_id, colour_id, colour_range_id, company_id, " +
                "count_on_us_id, create_dte, dept_id, discipline, domain_id, flavour_dsc, flavour_group_id, " +
                "flavour_id, flavour_range_id, forecast_ind, free_range_id, from_temp, group_id, high_max_temp, " +
                "high_min_temp, item_grandparent, item_parent, kidz_id, orderable_ind, pack_ind, pack_member, pack_qty, " +
                "phase_id, price_mark_ind, primary_ref_item_ind, primary_size_dsc, primary_size_group_id, primary_size_id, " +
                "primary_size_range_id, product_group_scaling, product_id, reference_item_ind, scent_dsc, scent_group_id, " +
                "scent_id, scent_range_id, season_id, secondary_size_dsc, secondary_size_group_id, secondary_size_id, " +
                "secondary_size_range_id, sellable_ind, short_dsc, simple_pack_ind, size_profile_ind, standard_uom, " +
                "status, sub_group_id, subclass_id, supplier_no, to_temp, tran_ind, tran_level, std_colour, std_size, " +
                "std_static_mass, std_style, std_style_colour, variable_weight_ind, loose_prod_ind, item_scale_ind, " +
                "legacy_sku_no, legacy_random_mass_ind, legacy_vat_ind, action_ind, extract_seq_no, vat_cde, vat_rate, " +
                "source_system, vpn_no, ext_ref_no, item_long_desc, segregation_ind, prod_class, last_update_dte) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                "ON DUPLICATE KEY UPDATE item_level = VALUES(item_level), item_number_type = VALUES(item_number_type), " +
                "prefix = VALUES(prefix), allocator_system = VALUES(allocator_system), business_unit_id = VALUES(business_unit_id), " +
                "catch_weight_ind = VALUES(catch_weight_ind), class_id = VALUES(class_id), colour_dsc = VALUES(colour_dsc), " +
                "colour_group_id = VALUES(colour_group_id), colour_id = VALUES(colour_id), colour_range_id = VALUES(colour_range_id), " +
                "company_id = VALUES(company_id), count_on_us_id = VALUES(count_on_us_id), create_dte = VALUES(create_dte), " +
                "dept_id = VALUES(dept_id), discipline = VALUES(discipline), domain_id = VALUES(domain_id), flavour_dsc = VALUES(flavour_dsc), " +
                "flavour_group_id = VALUES(flavour_group_id), flavour_id = VALUES(flavour_id), flavour_range_id = VALUES(flavour_range_id), " +
                "forecast_ind = VALUES(forecast_ind), free_range_id = VALUES(free_range_id), from_temp = VALUES(from_temp), " +
                "group_id = VALUES(group_id), high_max_temp = VALUES(high_max_temp), high_min_temp = VALUES(high_min_temp), " +
                "item_grandparent = VALUES(item_grandparent), item_parent = VALUES(item_parent), kidz_id = VALUES(kidz_id), " +
                "orderable_ind = VALUES(orderable_ind), pack_ind = VALUES(pack_ind), pack_member = VALUES(pack_member), " +
                "pack_qty = VALUES(pack_qty), phase_id = VALUES(phase_id), price_mark_ind = VALUES(price_mark_ind), " +
                "primary_ref_item_ind = VALUES(primary_ref_item_ind), primary_size_dsc = VALUES(primary_size_dsc), " +
                "primary_size_group_id = VALUES(primary_size_group_id), primary_size_id = VALUES(primary_size_id), " +
                "primary_size_range_id = VALUES(primary_size_range_id), product_group_scaling = VALUES(product_group_scaling), " +
                "product_id = VALUES(product_id), reference_item_ind = VALUES(reference_item_ind), scent_dsc = VALUES(scent_dsc), " +
                "scent_group_id = VALUES(scent_group_id), scent_id = VALUES(scent_id), scent_range_id = VALUES(scent_range_id), " +
                "season_id = VALUES(season_id), secondary_size_dsc = VALUES(secondary_size_dsc), secondary_size_group_id = VALUES(secondary_size_group_id), secondary_size_id = VALUES(secondary_size_id), secondary_size_range_id = VALUES(secondary_size_range_id), sellable_ind = VALUES(sellable_ind), short_dsc = VALUES(short_dsc), simple_pack_ind = VALUES(simple_pack_ind), size_profile_ind = VALUES(size_profile_ind), standard_uom = VALUES(standard_uom), status = VALUES(status), sub_group_id = VALUES(sub_group_id), subclass_id = VALUES(subclass_id), supplier_no = VALUES(supplier_no), to_temp = VALUES(to_temp), tran_ind = VALUES(tran_ind), tran_level = VALUES(tran_level), std_colour = VALUES(std_colour), std_size = VALUES(std_size), std_static_mass = VALUES(std_static_mass), std_style = VALUES(std_style), std_style_colour = VALUES(std_style_colour), variable_weight_ind = VALUES(variable_weight_ind), loose_prod_ind = VALUES(loose_prod_ind), item_scale_ind = VALUES(item_scale_ind), legacy_sku_no = VALUES(legacy_sku_no), legacy_random_mass_ind = VALUES(legacy_random_mass_ind), legacy_vat_ind = VALUES(legacy_vat_ind), action_ind = VALUES(action_ind), extract_seq_no = VALUES(extract_seq_no), vat_cde = VALUES(vat_cde), vat_rate = VALUES(vat_rate), source_system = VALUES(source_system), vpn_no = VALUES(vpn_no), ext_ref_no = VALUES(ext_ref_no), item_long_desc = VALUES(item_long_desc), segregation_ind = VALUES(segregation_ind), prod_class = VALUES(prod_class), last_update_dte = VALUES(last_update_dte)";
    }

    /**
     * Executes the job: builds a Flink streaming pipeline that consumes from
     * the configured Kafka topic, maps each message onto an {@link Item} and
     * upserts it into the configured MySQL table. Blocks until the Flink job
     * terminates (normally only via cancellation, since this is an unbounded
     * source).
     */
    public void run()
    {
        try
        {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            log.debug("KafkaItemToMysqlJob: starting to consume from Kafka topic {}.", topicName);

            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setTopics(topicName)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> messageStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            messageStream
                    .map(new ItemFromJsonFunction())
                    .filter(item -> item != null)
                    .addSink(JdbcSink.sink(
                            buildInsertQuery(),
                            (ps, item) ->
                            {
                                try
                                {
                                    ps.setString(1, item.getItemId());
                                    ps.setInt(2, item.getItemLevel());
                                    ps.setString(3, item.getItemNumberType());
                                    ps.setInt(4, item.getPrefix());
                                    ps.setString(5, item.getAllocatorSystem());
                                    ps.setInt(6, item.getBusinessUnitId());
                                    ps.setString(7, item.getCatchWeightInd());
                                    ps.setInt(8, item.getClassId());
                                    ps.setString(9, item.getColourDsc());
                                    ps.setString(10, item.getColourGroupId());
                                    ps.setString(11, item.getColourId());
                                    ps.setInt(12, item.getColourRangeId());
                                    ps.setInt(13, item.getCompanyId());
                                    ps.setString(14, item.getCountOnUsId());
                                    ps.setObject(15, item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                                    ps.setInt(16, item.getDeptId());
                                    ps.setString(17, item.getDiscipline());
                                    ps.setInt(18, item.getDomainId());
                                    ps.setString(19, item.getFlavourDsc());
                                    ps.setString(20, item.getFlavourGroupId());
                                    ps.setString(21, item.getFlavourId());
                                    ps.setInt(22, item.getFlavourRangeId());
                                    ps.setString(23, item.getForecastInd());
                                    ps.setString(24, item.getFreeRangeId());
                                    ps.setInt(25, item.getFromTemp());
                                    ps.setInt(26, item.getGroupId());
                                    ps.setInt(27, item.getHighMaxTemp());
                                    ps.setInt(28, item.getHighMinTemp());
                                    ps.setString(29, item.getItemGrandparent());
                                    ps.setString(30, item.getItemParent());
                                    ps.setString(31, item.getKidzId());
                                    ps.setString(32, item.getOrderableInd());
                                    ps.setString(33, item.getPackInd());
                                    ps.setString(34, item.getPackMember());
                                    ps.setBigDecimal(35, item.getPackQty());
                                    ps.setInt(36, item.getPhaseId());
                                    ps.setString(37, item.getPriceMarkInd());
                                    ps.setString(38, item.getPrimaryRefItemInd());
                                    ps.setString(39, item.getPrimarySizeDsc());
                                    ps.setString(40, item.getPrimarySizeGroupId());
                                    ps.setString(41, item.getPrimarySizeId());
                                    ps.setInt(42, item.getPrimarySizeRangeId());
                                    ps.setString(43, item.getProductGroupScaling());
                                    ps.setString(44, item.getProductId());
                                    ps.setString(45, item.getReferenceItemInd());
                                    ps.setString(46, item.getScentDsc());
                                    ps.setString(47, item.getScentGroupId());
                                    ps.setString(48, item.getScentId());
                                    ps.setInt(49, item.getScentRangeId());
                                    ps.setInt(50, item.getSeasonId());
                                    ps.setString(51, item.getSecondarySizeDsc());
                                    ps.setString(52, item.getSecondarySizeGroupId());
                                    ps.setString(53, item.getSecondarySizeId());
                                    ps.setInt(54, item.getSecondarySizeRangeId());
                                    ps.setString(55, item.getSellableInd());
                                    ps.setString(56, item.getShortDsc());
                                    ps.setString(57, item.getSimplePackInd());
                                    ps.setString(58, item.getSizeProfileInd());
                                    ps.setString(59, item.getStandardUom());
                                    ps.setString(60, item.getStatus());
                                    ps.setInt(61, item.getSubGroupId());
                                    ps.setInt(62, item.getSubclassId());
                                    ps.setInt(63, item.getSupplierNo());
                                    ps.setInt(64, item.getToTemp());
                                    ps.setString(65, item.getTranInd());
                                    ps.setInt(66, item.getTranLevel());
                                    ps.setString(67, item.getStdColour());
                                    ps.setString(68, item.getStdSize());
                                    ps.setBigDecimal(69, item.getStdStaticMass());
                                    ps.setString(70, item.getStdStyle());
                                    ps.setString(71, item.getStdStyleColour());
                                    ps.setString(72, String.valueOf(item.getVariableWeightInd()));
                                    ps.setString(73, String.valueOf(item.getLooseProdInd()));
                                    ps.setString(74, String.valueOf(item.getItemScaleInd()));
                                    ps.setString(75, item.getLegacySkuNo());
                                    ps.setString(76, String.valueOf(item.getLegacyRandomMassInd()));
                                    ps.setString(77, String.valueOf(item.getLegacyVatInd()));
                                    ps.setString(78, String.valueOf(item.getActionInd()));
                                    ps.setLong(79, item.getExtractSeqNo());
                                    ps.setString(80, item.getVatCde());
                                    ps.setBigDecimal(81, item.getVatRate());
                                    ps.setString(82, item.getSourceSystem());
                                    ps.setString(83, item.getVpnNo());
                                    ps.setString(84, item.getExtRefNo());
                                    ps.setString(85, item.getItemLongDesc());
                                    ps.setString(86, item.getSegregationInd());
                                    ps.setString(87, item.getProdClass());
                                    ps.setObject(88, item.getLastUpdateDte());
                                }
                                catch (Exception e)
                                {
                                    log.error("Error preparing statement for item {}: {}", item.getItemId(), e.getMessage(), e);
                                }
                            },
                            JdbcExecutionOptions.builder()
                                    .withBatchSize(1000)
                                    .withBatchIntervalMs(200)
                                    .withMaxRetries(3)
                                    .build(),
                            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl(mysqlJdbcUrl)
                                    .withDriverName("com.mysql.cj.jdbc.Driver")
                                    .withUsername(mysqlUsername)
                                    .withPassword(mysqlPassword)
                                    .build()
                    ));

            log.debug("KafkaItemToMysqlJob pipeline built; invoking env.execute()...");
            env.execute("KafkaItemToMysqlJob: Kafka to MySQL");
            log.debug("KafkaItemToMysqlJob completed.");
        }
        catch (Exception e)
        {
            log.error("KafkaItemToMysqlJob failed: {}", e.getMessage(), e);
        }
    }

    /**
     * Serializable Flink {@link MapFunction} that deserializes a Kafka JSON
     * payload into an {@link Item}, defaulting any missing/null fields to a
     * safe value so that a partial upstream record can still be persisted.
     */
    public static class ItemFromJsonFunction implements MapFunction<String, Item>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;
        private int itemCounter = 0;

        /**
         * @param json the raw JSON message consumed from Kafka.
         * @return the deserialized, null-safe {@link Item}, or {@code null} if deserialization failed.
         */
        @Override
        public Item map(String json)
        {
            try
            {
                itemCounter++;
                log.debug("Received JSON item #{} from Kafka: {}", itemCounter, json);
                Item item = gson.fromJson(json, Item.class);
                if (item == null)
                {
                    log.error("Item pulled from Kafka topic was null / could not be deserialized: {}", json);
                    return null;
                }
                applyDefaults(item);
                return item;
            }
            catch (Exception e)
            {
                log.error("Deserialization failed for JSON: {}", json, e);
                return null;
            }
        }

        /** Fills in sensible defaults for any null fields so that the row can be safely inserted. */
        private void applyDefaults(Item item)
        {
            item.setItemId(item.getItemId() != null ? item.getItemId() : UUID.randomUUID().toString());
            item.setItemNumberType(item.getItemNumberType() != null ? item.getItemNumberType() : "UNKNOWN");
            item.setBusinessUnitId(item.getBusinessUnitId() != null ? item.getBusinessUnitId() : 0);
            item.setCreateDte(item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
            item.setItemLevel(item.getItemLevel() != null ? item.getItemLevel() : 0);
            item.setVatRate(item.getVatRate() != null ? item.getVatRate() : BigDecimal.ZERO);
            item.setStandardUom(item.getStandardUom() != null ? item.getStandardUom() : "EA");
            item.setPrefix(item.getPrefix() != null ? item.getPrefix() : 0);
            item.setAllocatorSystem(item.getAllocatorSystem() != null ? item.getAllocatorSystem() : "DEFAULT");
            item.setCatchWeightInd(item.getCatchWeightInd() != null ? item.getCatchWeightInd() : "N");
            item.setClassId(item.getClassId() != null ? item.getClassId() : 0);
            item.setColourGroupId(item.getColourGroupId() != null ? item.getColourGroupId() : "None");
            item.setColourId(item.getColourId() != null ? item.getColourId() : "UNKNOWN");
            item.setColourRangeId(item.getColourRangeId() != null ? item.getColourRangeId() : 0);
            item.setCompanyId(item.getCompanyId() != null ? item.getCompanyId() : 0);
            item.setDeptId(item.getDeptId() != null ? item.getDeptId() : 0);
            item.setDomainId(item.getDomainId() != null ? item.getDomainId() : 0);
            item.setFlavourRangeId(item.getFlavourRangeId() != null ? item.getFlavourRangeId() : 0);
            item.setForecastInd(item.getForecastInd() != null ? item.getForecastInd() : "N");
            item.setFromTemp(item.getFromTemp() != null ? item.getFromTemp() : 0);
            item.setGroupId(item.getGroupId() != null ? item.getGroupId() : 0);
            item.setHighMaxTemp(item.getHighMaxTemp() != null ? item.getHighMaxTemp() : 0);
            item.setHighMinTemp(item.getHighMinTemp() != null ? item.getHighMinTemp() : 0);
            item.setOrderableInd(item.getOrderableInd() != null ? item.getOrderableInd() : "N");
            item.setPackInd(item.getPackInd() != null ? item.getPackInd() : "N");
            item.setPhaseId(item.getPhaseId() != null ? item.getPhaseId() : 1);
            item.setPriceMarkInd(item.getPriceMarkInd() != null ? item.getPriceMarkInd() : "N");
            item.setPrimaryRefItemInd(item.getPrimaryRefItemInd() != null ? item.getPrimaryRefItemInd() : "N");
            item.setPrimarySizeRangeId(item.getPrimarySizeRangeId() != null ? item.getPrimarySizeRangeId() : 0);
            item.setReferenceItemInd(item.getReferenceItemInd() != null ? item.getReferenceItemInd() : "N");
            item.setScentRangeId(item.getScentRangeId() != null ? item.getScentRangeId() : 0);
            item.setSeasonId(item.getSeasonId() != null ? item.getSeasonId() : 0);
            item.setSecondarySizeRangeId(item.getSecondarySizeRangeId() != null ? item.getSecondarySizeRangeId() : 0);
            item.setSellableInd(item.getSellableInd() != null ? item.getSellableInd() : "N");
            item.setShortDsc(item.getShortDsc() != null ? item.getShortDsc() : "N/A");
            item.setSimplePackInd(item.getSimplePackInd() != null ? item.getSimplePackInd() : "N");
            item.setSizeProfileInd(item.getSizeProfileInd() != null ? item.getSizeProfileInd() : "N");
            item.setStatus(item.getStatus() != null ? item.getStatus() : "A");
            item.setSubGroupId(item.getSubGroupId() != null ? item.getSubGroupId() : 0);
            item.setSubclassId(item.getSubclassId() != null ? item.getSubclassId() : 0);
            item.setSupplierNo(item.getSupplierNo() != null ? item.getSupplierNo() : 0);
            item.setToTemp(item.getToTemp() != null ? item.getToTemp() : 0);
            item.setTranInd(item.getTranInd() != null ? item.getTranInd() : "N");
            item.setTranLevel(item.getTranLevel() != null ? item.getTranLevel() : 0);
            item.setStdColour(item.getStdColour() != null ? item.getStdColour() : "UNKNOWN");
            item.setStdSize(item.getStdSize() != null ? item.getStdSize() : "UNKNOWN");
            item.setStdStaticMass(item.getStdStaticMass() != null ? item.getStdStaticMass() : BigDecimal.ZERO);
            item.setStdStyle(item.getStdStyle() != null ? item.getStdStyle() : UUID.randomUUID().toString());
            item.setStdStyleColour(item.getStdStyleColour() != null ? item.getStdStyleColour() : "UNKNOWN");
            item.setLegacySkuNo(item.getLegacySkuNo() != null ? item.getLegacySkuNo() : "N/A");
            item.setVariableWeightInd(item.getVariableWeightInd() != null ? item.getVariableWeightInd() : 'N');
            item.setLooseProdInd(item.getLooseProdInd() != null ? item.getLooseProdInd() : 'N');
            item.setItemScaleInd(item.getItemScaleInd() != null ? item.getItemScaleInd() : 'N');
            item.setLegacyRandomMassInd(item.getLegacyRandomMassInd() != null ? item.getLegacyRandomMassInd() : 'N');
            item.setLegacyVatInd(item.getLegacyVatInd() != null ? item.getLegacyVatInd() : 'N');
            item.setActionInd(item.getActionInd() != null ? item.getActionInd() : 'N');
            item.setExtractSeqNo(item.getExtractSeqNo() != null ? item.getExtractSeqNo() : 0L);
            item.setVatCde(item.getVatCde() != null ? item.getVatCde() : "S");
            item.setSourceSystem(item.getSourceSystem() != null ? item.getSourceSystem() : "UNKNOWN");
            item.setVpnNo(item.getVpnNo() != null ? item.getVpnNo() : "N/A");
            item.setExtRefNo(item.getExtRefNo() != null ? item.getExtRefNo() : "N/A");
            item.setItemLongDesc(item.getItemLongDesc() != null ? item.getItemLongDesc() : "No Description");
            item.setSegregationInd(item.getSegregationInd() != null ? item.getSegregationInd() : "N");
            item.setProdClass(item.getProdClass() != null ? item.getProdClass() : "N/A");
            item.setLastUpdateDte(item.getLastUpdateDte() != null ? item.getLastUpdateDte() : LocalDateTime.now());
        }
    }
}

