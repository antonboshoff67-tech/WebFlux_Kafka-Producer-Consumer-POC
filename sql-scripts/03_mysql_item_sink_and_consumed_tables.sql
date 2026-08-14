-- ============================================================================
-- MySQL: SINK tables written to by the Kafka -> Apache Flink pipeline
-- ----------------------------------------------------------------------------
-- Two tables are created here:
--
-- 1) item_poc.ITEM
--    The table upserted into by the Flink "KafkaItemToMysqlJob" via
--    INSERT ... ON DUPLICATE KEY UPDATE. This is the actual, wired-in
--    destination table for every Item record consumed from the shared Kafka
--    topic. Column list matches Item.java exactly (see DATABASE_SETUP.md
--    section 3). Left EMPTY here on purpose - it gets populated by running
--    the pipeline (POST /item-kafka/app/publish-items/v1 then
--    POST /flink/start-job2), not by a static seed script.
--
-- 2) item_poc.ITEM_CONSUMED
--    An additional, optional audit-trail table that records *when* and via
--    *which consumer group* an Item was consumed, on top of the same column
--    set. Not currently wired into KafkaItemToMysqlJob/ItemConsumerService -
--    provided as a ready-to-use extension point if you want to track
--    consumption history (e.g. for reconciliation/dedupe reporting) separate
--    from the plain upsert-only ITEM sink table.
--
-- Usage:
--   mysql -u root -p < 03_mysql_item_sink_and_consumed_tables.sql
-- ============================================================================

CREATE DATABASE IF NOT EXISTS item_poc CHARACTER SET utf8mb4;
USE item_poc;

-- 1) Sink table used by KafkaItemToMysqlJob
CREATE TABLE IF NOT EXISTS ITEM (
    item_id                   VARCHAR(64) PRIMARY KEY,
    item_level                INT,
    item_number_type          VARCHAR(32),
    prefix                    INT,
    allocator_system          VARCHAR(64),
    business_unit_id          INT,
    catch_weight_ind          VARCHAR(1),
    class_id                  INT,
    colour_dsc                VARCHAR(128),
    colour_group_id           VARCHAR(32),
    colour_id                 VARCHAR(32),
    colour_range_id           INT,
    company_id                INT,
    count_on_us_id            VARCHAR(32),
    create_dte                DATETIME,
    dept_id                   INT,
    discipline                VARCHAR(32),
    domain_id                 INT,
    flavour_dsc               VARCHAR(128),
    flavour_group_id          VARCHAR(32),
    flavour_id                VARCHAR(32),
    flavour_range_id          INT,
    forecast_ind              VARCHAR(1),
    free_range_id             VARCHAR(32),
    from_temp                 INT,
    group_id                  INT,
    high_max_temp             INT,
    high_min_temp             INT,
    item_grandparent          VARCHAR(64),
    item_parent               VARCHAR(64),
    kidz_id                   VARCHAR(32),
    orderable_ind             VARCHAR(1),
    pack_ind                  VARCHAR(1),
    pack_member               VARCHAR(32),
    pack_qty                  DECIMAL(18,4),
    phase_id                  INT,
    price_mark_ind            VARCHAR(1),
    primary_ref_item_ind      VARCHAR(1),
    primary_size_dsc          VARCHAR(64),
    primary_size_group_id     VARCHAR(32),
    primary_size_id           VARCHAR(32),
    primary_size_range_id     INT,
    product_group_scaling     VARCHAR(32),
    product_id                VARCHAR(64),
    reference_item_ind        VARCHAR(1),
    scent_dsc                 VARCHAR(64),
    scent_group_id            VARCHAR(32),
    scent_id                  VARCHAR(32),
    scent_range_id            INT,
    season_id                 INT,
    secondary_size_dsc        VARCHAR(64),
    secondary_size_group_id   VARCHAR(32),
    secondary_size_id         VARCHAR(32),
    secondary_size_range_id   INT,
    sellable_ind              VARCHAR(1),
    short_dsc                 VARCHAR(128),
    simple_pack_ind           VARCHAR(1),
    size_profile_ind          VARCHAR(1),
    standard_uom              VARCHAR(16),
    status                    VARCHAR(16),
    sub_group_id              INT,
    subclass_id               INT,
    supplier_no               INT,
    to_temp                   INT,
    tran_ind                  VARCHAR(1),
    tran_level                INT,
    std_colour                 VARCHAR(32),
    std_size                   VARCHAR(32),
    std_static_mass            DECIMAL(18,4),
    std_style                  VARCHAR(64),
    std_style_colour           VARCHAR(64),
    variable_weight_ind       CHAR(1),
    loose_prod_ind            CHAR(1),
    item_scale_ind            CHAR(1),
    legacy_sku_no             VARCHAR(32),
    legacy_random_mass_ind    CHAR(1),
    legacy_vat_ind            CHAR(1),
    action_ind                CHAR(1),
    extract_seq_no            BIGINT,
    vat_cde                   VARCHAR(8),
    vat_rate                  DECIMAL(6,4),
    source_system             VARCHAR(32),
    vpn_no                    VARCHAR(32),
    ext_ref_no                VARCHAR(64),
    item_long_desc            VARCHAR(255),
    segregation_ind           VARCHAR(1),
    prod_class                VARCHAR(32),
    last_update_dte           DATETIME
);

-- 2) Optional audit/history table: one row per (item_id, consumer_group)
--    consumption event, in addition to the plain upsert-only ITEM table above.
CREATE TABLE IF NOT EXISTS ITEM_CONSUMED (
    consumed_seq_no  BIGINT AUTO_INCREMENT PRIMARY KEY,
    item_id          VARCHAR(64)   NOT NULL,
    consumer_group   VARCHAR(64)   NOT NULL,
    kafka_topic      VARCHAR(128)  NOT NULL DEFAULT 'Item_Topic',
    kafka_partition  INT           NULL,
    kafka_offset     BIGINT        NULL,
    short_dsc        VARCHAR(128)  NULL,
    status           VARCHAR(16)   NULL,
    consumed_at      DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_item_consumed_item_id (item_id),
    INDEX idx_item_consumed_group (consumer_group)
);

-- Recommended: dedicated MySQL user for both databases (see DATABASE_SETUP.md 3.1)
-- CREATE USER 'item_poc_user'@'%' IDENTIFIED BY 'change-me-strong-password';
-- GRANT ALL PRIVILEGES ON item_poc.* TO 'item_poc_user'@'%';
-- GRANT ALL PRIVILEGES ON item_poc_source.* TO 'item_poc_user'@'%';
-- FLUSH PRIVILEGES;

