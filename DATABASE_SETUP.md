# Database Setup

This project touches **two** databases:

1. **Source** - the original `ITEM` master-data table, read by `ItemRepository` (reactive R2DBC path) and by the Flink `MssqlItemToKafkaJob` (JDBC path). In the original corporate environment this was MS SQL Server, reached with **Windows Integrated Authentication**.
2. **Sink** - the MySQL `ITEM` table written to by the Flink `KafkaItemToMysqlJob`.

You can run this demo three ways:
- **Full split (closest to the original setup):** SQL Server as source, MySQL as sink.
- **MySQL-only:** use MySQL for both source and sink (simplest for anyone who doesn't have SQL Server available).
- **Sink-only:** skip the source table/`MssqlItemToKafkaJob` entirely and just publish sample `Item` JSON straight onto Kafka to exercise `KafkaItemToMysqlJob`.

Both full DDL scripts (SQL Server and MySQL) are below, generated directly from the column mappings in `src/main/java/com/antontech/webflux_kafka/model/Item.java`.

### Ready-to-run seed scripts

For a quick demo (e.g. to populate the React front end's Item grid), the `sql-scripts/` folder at the repository root contains ready-to-run scripts with **200 dummy `Item` records each** (`ITEM-0001` .. `ITEM-0200`):

| Script | Target | Purpose |
|---|---|---|
| `sql-scripts/01_mssql_item_seed_200.sql` | MS SQL Server | Creates `ItemPoc.dbo.ITEM` and seeds 200 rows. For anyone running the original SQL Server -> Kafka -> MySQL topology. |
| `sql-scripts/02_mysql_item_source_seed_200.sql` | MySQL | Creates `item_poc_source.ITEM` and seeds 200 rows. **This is what this POC's `dev` profile actually points `spring.datasource` at** - MySQL is used for the source table instead of SQL Server. |
| `sql-scripts/03_mysql_item_sink_and_consumed_tables.sql` | MySQL | Creates the `item_poc.ITEM` sink table (upserted into by `KafkaItemToMysqlJob`) plus an optional `item_poc.ITEM_CONSUMED` audit table that can record which consumer group consumed which item and when. |

Run with, e.g.:

```powershell
mysql -u root -p < sql-scripts\02_mysql_item_source_seed_200.sql
mysql -u root -p < sql-scripts\03_mysql_item_sink_and_consumed_tables.sql
```

```powershell
sqlcmd -S localhost -E -i sql-scripts\01_mssql_item_seed_200.sql
```

---

## 1. MS SQL Server source setup

### 1.1 What "Windows Integrated Authentication" means here

The original connection string used by this POC was:

```
jdbc:sqlserver://C21SQL04\AMOS.1:1433;databaseName=CS_Caissa_Central_Master_Data;integratedSecurity=true;encrypt=false;
```

`integratedSecurity=true` tells the Microsoft JDBC driver to authenticate using the **Windows account the Java process is running as**, instead of a SQL username/password embedded in the connection string. Under the hood this uses SSPI/Kerberos (or NTLM as a fallback) to negotiate credentials with SQL Server - no password ever appears in configuration.

Requirements for integrated security to work:
- The app must run on Windows (or a JVM with the SSPI native library available) as a Windows user/service account.
- That Windows account (or a Windows group it belongs to) must be granted a SQL Server login and at least `db_datareader` permission on the source database.
- SQL Server must be configured for **Windows Authentication** or **Mixed Mode**, and the account must not be locked out.
- The `mssql-jdbc_auth-<version>.x64.dll` native helper (already bundled under `src/main/resources/lib/`) must be reachable on the JVM's `java.library.path` - this is what performs the native SSPI handshake, since integrated security cannot be done in pure Java.

If any of that isn't available in your environment (e.g. you're on macOS/Linux, or don't have a domain-joined SQL Server), use SQL Server **Mixed Mode** with a normal username/password instead:

```
jdbc:sqlserver://<host>:1433;databaseName=<db>;encrypt=false;user=<sql-login>;password=<password>;
```

...or skip SQL Server entirely and use the MySQL-only path in section 2.

### 1.2 Granting your Windows account access (run once, as a SQL Server admin)

```sql
-- Run in SSMS or sqlcmd, connected as an existing sysadmin login
CREATE LOGIN [YOURDOMAIN\your-windows-account] FROM WINDOWS;
GO
USE ItemPoc;
GO
CREATE USER [YOURDOMAIN\your-windows-account] FOR LOGIN [YOURDOMAIN\your-windows-account];
GO
ALTER ROLE db_datareader ADD MEMBER [YOURDOMAIN\your-windows-account];
GO
```

For a local, single-machine demo, you can instead just grant your local Windows user (`.\your-username`) the same way, or simply run SQL Server Developer Edition locally and use `integratedSecurity=true` with your own Windows login, which SQL Server treats as trusted by default when installed with Windows Authentication mode.

### 1.3 SQL Server DDL script

```sql
-- 1) Create the database
IF DB_ID('ItemPoc') IS NULL
BEGIN
    CREATE DATABASE ItemPoc;
END
GO

USE ItemPoc;
GO

-- 2) Create the source ITEM table
IF OBJECT_ID('dbo.ITEM', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ITEM (
        item_id                   NVARCHAR(64)   NOT NULL PRIMARY KEY,
        item_level                INT            NULL,
        item_number_type          NVARCHAR(32)   NULL,
        prefix                    INT            NULL,
        allocator_system          NVARCHAR(64)   NULL,
        business_unit_id          INT            NULL,
        catch_weight_ind          NVARCHAR(1)    NULL,
        class_id                  INT            NULL,
        colour_dsc                NVARCHAR(128)  NULL,
        colour_group_id           NVARCHAR(32)   NULL,
        colour_id                 NVARCHAR(32)   NULL,
        colour_range_id           INT            NULL,
        company_id                INT            NULL,
        count_on_us_id            NVARCHAR(32)   NULL,
        create_dte                DATETIME2      NULL,
        dept_id                   INT            NULL,
        discipline                NVARCHAR(32)   NULL,
        domain_id                 INT            NULL,
        flavour_dsc               NVARCHAR(128)  NULL,
        flavour_group_id          NVARCHAR(32)   NULL,
        flavour_id                NVARCHAR(32)   NULL,
        flavour_range_id          INT            NULL,
        forecast_ind              NVARCHAR(1)    NULL,
        free_range_id             NVARCHAR(32)   NULL,
        from_temp                 INT            NULL,
        group_id                  INT            NULL,
        high_max_temp             INT            NULL,
        high_min_temp             INT            NULL,
        item_grandparent          NVARCHAR(64)   NULL,
        item_parent               NVARCHAR(64)   NULL,
        kidz_id                   NVARCHAR(32)   NULL,
        orderable_ind             NVARCHAR(1)    NULL,
        pack_ind                  NVARCHAR(1)    NULL,
        pack_member               NVARCHAR(32)   NULL,
        pack_qty                  DECIMAL(18,4)  NULL,
        phase_id                  INT            NULL,
        price_mark_ind            NVARCHAR(1)    NULL,
        primary_ref_item_ind      NVARCHAR(1)    NULL,
        primary_size_dsc          NVARCHAR(64)   NULL,
        primary_size_group_id     NVARCHAR(32)   NULL,
        primary_size_id           NVARCHAR(32)   NULL,
        primary_size_range_id     INT            NULL,
        product_group_scaling     NVARCHAR(32)   NULL,
        product_id                NVARCHAR(64)   NULL,
        reference_item_ind        NVARCHAR(1)    NULL,
        scent_dsc                 NVARCHAR(64)   NULL,
        scent_group_id            NVARCHAR(32)   NULL,
        scent_id                  NVARCHAR(32)   NULL,
        scent_range_id            INT            NULL,
        season_id                 INT            NULL,
        secondary_size_dsc        NVARCHAR(64)   NULL,
        secondary_size_group_id   NVARCHAR(32)   NULL,
        secondary_size_id         NVARCHAR(32)   NULL,
        secondary_size_range_id   INT            NULL,
        sellable_ind              NVARCHAR(1)    NULL,
        short_dsc                 NVARCHAR(128)  NULL,
        simple_pack_ind           NVARCHAR(1)    NULL,
        size_profile_ind          NVARCHAR(1)    NULL,
        standard_uom              NVARCHAR(16)   NULL,
        status                    NVARCHAR(16)   NULL,
        sub_group_id              INT            NULL,
        subclass_id               INT            NULL,
        supplier_no               INT            NULL,
        to_temp                   INT            NULL,
        tran_ind                  NVARCHAR(1)    NULL,
        tran_level                INT            NULL,
        ww_colour                 NVARCHAR(32)   NULL,
        ww_size                   NVARCHAR(32)   NULL,
        ww_static_mass            DECIMAL(18,4)  NULL,
        ww_style                  NVARCHAR(64)   NULL,
        ww_style_colour           NVARCHAR(64)   NULL,
        variable_weight_ind       CHAR(1)        NULL,
        loose_prod_ind            CHAR(1)        NULL,
        item_scale_ind            CHAR(1)        NULL,
        legacy_sku_no             NVARCHAR(32)   NULL,
        legacy_random_mass_ind    CHAR(1)        NULL,
        legacy_vat_ind            CHAR(1)        NULL,
        action_ind                CHAR(1)        NULL,
        extract_seq_no            BIGINT         NULL,
        vat_cde                   NVARCHAR(8)    NULL,
        vat_rate                  DECIMAL(6,4)   NULL,
        source_system             NVARCHAR(32)   NULL,
        vpn_no                    NVARCHAR(32)   NULL,
        ext_ref_no                NVARCHAR(64)   NULL,
        item_long_desc            NVARCHAR(255)  NULL,
        segregation_ind           NVARCHAR(1)    NULL,
        prod_class                NVARCHAR(32)   NULL,
        last_update_dte           DATETIME2      NULL
    );
END
GO

-- 3) Seed a few sample rows so the producer/Flink Job 1 have something to read
INSERT INTO dbo.ITEM (item_id, item_level, item_number_type, allocator_system, standard_uom, status, short_dsc, create_dte, last_update_dte)
VALUES
    ('ITEM-0001', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 1', SYSDATETIME(), SYSDATETIME()),
    ('ITEM-0002', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 2', SYSDATETIME(), SYSDATETIME()),
    ('ITEM-0003', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 3', SYSDATETIME(), SYSDATETIME());
GO
```

Set `ITEM_MSSQL_URL` to point at this database, e.g.:

```
ITEM_MSSQL_URL=jdbc:sqlserver://localhost:1433;databaseName=ItemPoc;integratedSecurity=true;encrypt=false;
```

---

## 2. MySQL alternative for the source table (no SQL Server needed)

If you don't have SQL Server available, point the **reactive source** to MySQL instead by setting `ITEM_R2DBC_URL` to your MySQL source database. Keep `ITEM_MSSQL_URL` for Flink Job 1 only if you still want to run the SQL Server -> Kafka path; otherwise you can skip Job 1 and use the MySQL source + reactive producer endpoints.

```sql
CREATE DATABASE IF NOT EXISTS item_poc_source CHARACTER SET utf8mb4;
USE item_poc_source;

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
    ww_colour                 VARCHAR(32),
    ww_size                   VARCHAR(32),
    ww_static_mass            DECIMAL(18,4),
    ww_style                  VARCHAR(64),
    ww_style_colour           VARCHAR(64),
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

INSERT INTO ITEM (item_id, item_level, item_number_type, allocator_system, standard_uom, status, short_dsc, create_dte, last_update_dte)
VALUES
    ('ITEM-0001', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 1', NOW(), NOW()),
    ('ITEM-0002', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 2', NOW(), NOW()),
    ('ITEM-0003', 1, 'EAN13', 'DEFAULT', 'EA', 'A', 'Sample demo item 3', NOW(), NOW());
```

---

## 3. MySQL sink table (used by `KafkaItemToMysqlJob`)

This is the table the Flink Kafka-to-MySQL job upserts into via `INSERT ... ON DUPLICATE KEY UPDATE`. Column list matches `Item.java` exactly and matches the parameter order used in `KafkaItemToMysqlJob#buildInsertQuery()`.

```sql
CREATE DATABASE IF NOT EXISTS item_poc CHARACTER SET utf8mb4;
USE item_poc;

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
    ww_colour                 VARCHAR(32),
    ww_size                   VARCHAR(32),
    ww_static_mass            DECIMAL(18,4),
    ww_style                  VARCHAR(64),
    ww_style_colour           VARCHAR(64),
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
```

Set:

```powershell
$env:ITEM_MYSQL_URL      = "jdbc:mysql://localhost:3306/item_poc?useSSL=false&allowPublicKeyRetrieval=true"
$env:ITEM_MYSQL_USERNAME = "item_poc_user"
$env:ITEM_MYSQL_PASSWORD = "<your-mysql-password>"
```

### 3.1 Creating a dedicated MySQL user (recommended over using `root`)

```sql
CREATE USER 'item_poc_user'@'%' IDENTIFIED BY 'change-me-strong-password';
GRANT ALL PRIVILEGES ON item_poc.* TO 'item_poc_user'@'%';
FLUSH PRIVILEGES;
```

---

## 4. Quick reference: which table is used by what

| Table | Database | Read/written by |
|---|---|---|
| `ITEM` (source) | SQL Server (or MySQL alternative, section 2) | `ItemRepository` / R2DBC (`ItemProducerController`), `MssqlItemToKafkaJob` |
| `ITEM` (sink) | MySQL (section 3) | `KafkaItemToMysqlJob` only |

They can be the *same* physical MySQL database/table if you only care about a simple end-to-end MySQL-to-MySQL demo, but keeping them separate (as shown above, `item_poc_source` vs `item_poc`) more faithfully mirrors the original two-database architecture and avoids the source and sink jobs racing each other on the same rows.

