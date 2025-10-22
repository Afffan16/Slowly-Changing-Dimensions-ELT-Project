-----------------------------------------------------------------------------

            -- SETTING UP THINGS --

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS PROJECT_06;
USE DATABASE PROJECT_06;
CREATE SCHEMA IF NOT EXISTS SCD2;
USE SCHEMA SCD2;
SHOW TABLES;

CREATE OR REPLACE TABLE customer 
(
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     update_timestamp timestamp_ntz default current_timestamp()
);
     

CREATE OR REPLACE TABLE customer_history
(
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar,
     start_time timestamp_ntz default current_timestamp(),
     end_time timestamp_ntz default current_timestamp(),
     is_current boolean
);
     
CREATE OR REPLACE TABLE customer_raw 
(
     customer_id number,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar
);
     
CREATE OR REPLACE STREAM customer_table_changes ON TABLE customer;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = CSV
    field_delimiter = ','
    skip_header = 1;    


SHOW TABLES;

---------------------------------------------------------------------

                    -- DATA RETRIEVAL SYSTEM --

CREATE OR REPLACE STAGE customer_ext_stage
  url='s3://cde-affan-assignemtns-bucket/'
  credentials=(aws_key_id='' aws_secret_key='')
  file_format = CSV_FORMAT;
  
SHOW STAGES;
LIST @customer_ext_stage;
DESC PIPE customer_s3_pipe

CREATE OR REPLACE PIPE customer_s3_pipe
  auto_ingest = true
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  file_format = CSV_FORMAT
  PATTERN = '.*customer_.*\\.csv';
  
SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('customer_s3_pipe');

select * from customer_raw
SELECT COUNT(*) FROM customer_raw;

SELECT COUNT(*) FROM customer;
TRUNCATE TABLE customer; TRUNCATE TABLE customer_raw; TRUNCATE TABLE customer_history;

-------------------------------------------------------------------------------------------

                                    -- SCD --
                                   -- SCD 01 --
               
MERGE INTO customer c
USING customer_raw cr
ON c.customer_id = cr.customer_id
WHEN MATCHED AND (
       c.first_name <> cr.first_name OR
       c.last_name  <> cr.last_name  OR
       c.email      <> cr.email      OR
       c.street     <> cr.street     OR
       c.city       <> cr.city       OR
       c.state      <> cr.state      OR
       c.country    <> cr.country
) THEN
    UPDATE SET
        c.first_name = cr.first_name,
        c.last_name  = cr.last_name,
        c.email      = cr.email,
        c.street     = cr.street,
        c.city       = cr.city,
        c.state      = cr.state,
        c.country    = cr.country,
        c.update_timestamp = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, last_name, email, street, city, state, country)
    VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);

CREATE OR REPLACE PROCEDURE pdr_scd_demo()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    try {
        // Merge statement with NVL for NULL handling
        var cmd = `
            MERGE INTO customer c
            USING customer_raw cr
                ON c.customer_id = cr.customer_id
            WHEN MATCHED AND (
                   NVL(c.first_name, '') <> NVL(cr.first_name, '') OR
                   NVL(c.last_name, '')  <> NVL(cr.last_name, '')  OR
                   NVL(c.email, '')      <> NVL(cr.email, '')      OR
                   NVL(c.street, '')     <> NVL(cr.street, '')     OR
                   NVL(c.city, '')       <> NVL(cr.city, '')       OR
                   NVL(c.state, '')      <> NVL(cr.state, '')      OR
                   NVL(c.country, '')    <> NVL(cr.country, '')
            ) THEN
                UPDATE SET
                    c.first_name  = cr.first_name,
                    c.last_name   = cr.last_name,
                    c.email       = cr.email,
                    c.street      = cr.street,
                    c.city        = cr.city,
                    c.state       = cr.state,
                    c.country     = cr.country,
                    c.update_timestamp = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (customer_id, first_name, last_name, email, street, city, state, country, update_timestamp)
                VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country, CURRENT_TIMESTAMP());
        `;
        
        // Truncate statement
        var cmd1 = `TRUNCATE TABLE customer_raw;`;

        // Execute merge
        var sql = snowflake.createStatement({sqlText: cmd});
        var result = sql.execute();
        
        // Get rows affected
        var rows_updated = 0;
        var rows_inserted = 0;
        if (result.next()) {
            rows_updated = result.getColumnValue(1); // Number of rows updated
            rows_inserted = result.getColumnValue(2); // Number of rows inserted
        }

        // Execute truncate
        var sql1 = snowflake.createStatement({sqlText: cmd1});
        sql1.execute();

        // Return detailed success message
        return `✅ SCD1 Merge Completed: ${rows_updated} rows updated, ${rows_inserted} rows inserted, staging table cleared.`;
    } catch (err) {
        // Return error message if something fails
        return `❌ SCD1 Merge Failed: ${err.message}`;
    }
$$;


-- Test the procedure
CALL pdr_scd_demo();


USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE TASKADMIN;

-- switch role to ACCOUNTADMIN to grant privileges
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TASKADMIN;

-- grant taskadmin role to sysadmin
USE ROLE SECURITYADMIN;
GRANT ROLE TASKADMIN TO ROLE SYSADMIN;


CREATE OR REPLACE TASK tsk_scd_raw
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL pdr_scd_demo();

-- Resume the task
ALTER TASK tsk_scd_raw SUSPEND;

-- Check task status
SHOW TASKS;


SELECT 
  TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) AS NEXT_RUN_SECONDS,
  SCHEDULED_TIME,
  CURRENT_TIMESTAMP,
  NAME,
  STATE 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;


SELECT COUNT(*) FROM CUSTOMER;
SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = 0;
     

-------------------------------------------------------------------------------------------

                                   -- SCD 02 --

SHOW STREAMS;
SELECT * FROM CUSTOMER_TABLE_CHANGES;

INSERT INTO CUSTOMER VALUES(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut','Cape Verde',current_timestamp());

update customer set FIRST_NAME='Jessica', update_timestamp = current_timestamp()::timestamp_ntz where customer_id=72;
delete from customer where customer_id =73 ;

select * from customer_history where customer_id in (72,73,223136);
select * from customer_table_changes;
select * from customer where customer_id in (72,73,223136);

DROP STREAM IF EXISTS customer_table_changes;
CREATE OR REPLACE STREAM customer_table_changes ON TABLE customer;
 
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' FOR TABLE customer;


CREATE OR REPLACE VIEW SCD_DEMO.SCD2.v_customer_change_data AS
-- Handle INSERTs (new records)
SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY, 
    STATE, 
    COUNTRY,
    update_timestamp AS start_time,
    CASE WHEN LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) IS NULL 
         THEN '9999-12-31'::TIMESTAMP_NTZ 
         ELSE LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) 
    END AS end_time,
    CASE WHEN LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) IS NULL 
         THEN TRUE 
         ELSE FALSE 
    END AS is_current,
    'I' AS dml_type
FROM (
    SELECT 
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME, 
        EMAIL, 
        STREET, 
        CITY, 
        STATE, 
        COUNTRY, 
        UPDATE_TIMESTAMP
    FROM SCD_DEMO.SCD2.customer_table_changes
    WHERE metadata$action = 'INSERT'
    AND metadata$isupdate = FALSE
)
UNION
-- Handle UPDATEs (insert new version and update old version)
SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY, 
    STATE, 
    COUNTRY, 
    start_time, 
    end_time, 
    is_current, 
    dml_type
FROM (
    SELECT 
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME, 
        EMAIL, 
        STREET, 
        CITY, 
        STATE, 
        COUNTRY,
        update_timestamp AS start_time,
        CASE WHEN LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) IS NULL 
             THEN '9999-12-31'::TIMESTAMP_NTZ 
             ELSE LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) 
        END AS end_time,
        CASE WHEN LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) IS NULL 
             THEN TRUE 
             ELSE FALSE 
        END AS is_current,
        dml_type
    FROM (
        -- Identify data to insert into customer_history
        SELECT 
            CUSTOMER_ID, 
            FIRST_NAME, 
            LAST_NAME, 
            EMAIL, 
            STREET, 
            CITY, 
            STATE, 
            COUNTRY, 
            update_timestamp, 
            'I' AS dml_type
        FROM SCD_DEMO.SCD2.customer_table_changes
        WHERE metadata$action = 'INSERT'
        AND metadata$isupdate = TRUE
        UNION
        -- Identify data in customer_history to update
        SELECT 
            CUSTOMER_ID, 
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
            start_time, 
            'U' AS dml_type
        FROM SCD_DEMO.SCD2.customer_history
        WHERE customer_id IN (
            SELECT DISTINCT customer_id 
            FROM SCD_DEMO.SCD2.customer_table_changes
            WHERE metadata$action = 'DELETE'
            AND metadata$isupdate = TRUE
        )
        AND is_current = TRUE
    )
)
UNION
-- Handle DELETEs
SELECT 
    ctc.CUSTOMER_ID, 
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
    ch.start_time, 
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS end_time, 
    FALSE AS is_current, 
    'D' AS dml_type
FROM SCD_DEMO.SCD2.customer_history ch
INNER JOIN SCD_DEMO.SCD2.customer_table_changes ctc
    ON ch.customer_id = ctc.customer_id
WHERE ctc.metadata$action = 'DELETE'
AND ctc.metadata$isupdate = FALSE
AND ch.is_current = TRUE;


select * from v_customer_change_data;

create or replace task tsk_scd_hist warehouse= COMPUTE_WH schedule='1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
merge into customer_history ch -- Target table to merge changes from NATION into
using v_customer_change_data ccd -- v_customer_change_data is a view that holds the logic that determines what to insert/update into the customer_history table.
   on ch.CUSTOMER_ID = ccd.CUSTOMER_ID -- CUSTOMER_ID and start_time determine whether there is a unique record in the customer_history table
   and ch.start_time = ccd.start_time
when matched and ccd.dml_type = 'U' then update -- Indicates the record has been updated and is no longer current and the end_time needs to be stamped
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when matched and ccd.dml_type = 'D' then update -- Deletes are essentially logical deletes. The record is stamped and no newer version is inserted
   set ch.end_time = ccd.end_time,
       ch.is_current = FALSE
when not matched and ccd.dml_type = 'I' then insert -- Inserting a new CUSTOMER_ID and updating an existing one both result in an insert
          (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time, is_current)
    values (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY,ccd.STATE,ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);
    
show tasks;
alter task tsk_scd_hist suspend;--resume --suspend



insert into customer values(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());
update customer set FIRST_NAME='Jessica' where customer_id=7523;
delete from customer where customer_id =136 and FIRST_NAME = 'Kim';
select count(*),customer_id from customer group by customer_id having count(*)=1;
select * from customer_history where customer_id =223136;
select * from customer_history where IS_CURRENT=TRUE;

--alter warehouse suspend;
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;

select * from customer_history where IS_CURRENT=FALSE;
show tasks;


----------------------------------------------------------------------------------------------------

                                -- SCD 03 --

CREATE OR REPLACE TABLE customer_scd3
(
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    previous_email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    update_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE PROCEDURE pdr_scd3_demo()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var cmd = `
        MERGE INTO customer_scd3 c 
        USING customer_raw cr
            ON c.customer_id = cr.customer_id
        WHEN MATCHED AND (
            c.first_name <> cr.first_name OR
            c.last_name  <> cr.last_name  OR
            c.email      <> cr.email      OR
            c.street     <> cr.street     OR
            c.city       <> cr.city       OR
            c.state      <> cr.state      OR
            c.country    <> cr.country
        ) THEN UPDATE
            SET c.previous_email = c.email,
                c.email          = cr.email,
                c.first_name     = cr.first_name,
                c.last_name      = cr.last_name,
                c.street         = cr.street,
                c.city           = cr.city,
                c.state          = cr.state,
                c.country        = cr.country,
                c.update_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (customer_id, first_name, last_name, email, previous_email, street, city, state, country)
            VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, NULL, cr.street, cr.city, cr.state, cr.country);
    `;
    var cmd1 = `TRUNCATE TABLE customer_raw;`;
    var sql = snowflake.createStatement({sqlText: cmd});
    var sql1 = snowflake.createStatement({sqlText: cmd1});
    var result = sql.execute();
    var result1 = sql1.execute();
    return cmd + '\n' + cmd1;
$$;

CREATE OR REPLACE TASK tsk_scd3
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
    CALL pdr_scd3_demo();

ALTER TASK tsk_scd3 SUSPEND; -- Start with suspended; use RESUME to activate
SHOW TASKS;

INSERT INTO customer_raw VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '123 Main St', 'New York', 'NY', 'USA');
                                
CALL pdr_scd3_demo();

SELECT * FROM customer_scd3 WHERE customer_id = 1;

INSERT INTO customer_raw VALUES
(1, 'John', 'Doe', 'john.doe2@example.com', '123 Main St', 'New York', 'NY', 'USA');

CALL pdr_scd3_demo();
SELECT * FROM customer_scd3 WHERE customer_id = 1;

----------------------------------------------------------------------------------------------------

                                -- SCD 04 --


CREATE OR REPLACE PROCEDURE pdr_scd4_current()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var cmd = `
        MERGE INTO customer c 
        USING customer_raw cr
            ON c.customer_id = cr.customer_id
        WHEN MATCHED THEN UPDATE
            SET c.first_name     = cr.first_name,
                c.last_name      = cr.last_name,
                c.email          = cr.email,
                c.street         = cr.street,
                c.city           = cr.city,
                c.state          = cr.state,
                c.country        = cr.country,
                c.update_timestamp = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (customer_id, first_name, last_name, email, street, city, state, country)
            VALUES (cr.customer_id, cr.first_name, cr.last_name, cr.email, cr.street, cr.city, cr.state, cr.country);
    `;
    var cmd1 = `TRUNCATE TABLE customer_raw;`;
    var sql = snowflake.createStatement({sqlText: cmd});
    var sql1 = snowflake.createStatement({sqlText: cmd1});
    var result = sql.execute();
    var result1 = sql1.execute();
    return cmd + '\n' + cmd1;
$$;

CREATE OR REPLACE VIEW v_customer_change_data_scd4 AS
-- Handle INSERTs (new records)
SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY, 
    STATE, 
    COUNTRY,
    update_timestamp AS start_time,
    '9999-12-31'::TIMESTAMP_NTZ AS end_time,
    TRUE AS is_current,
    'I' AS dml_type
FROM customer_table_changes
WHERE metadata$action = 'INSERT'
AND metadata$isupdate = FALSE
UNION
-- Handle UPDATEs (insert new version and update old version)
SELECT 
    CUSTOMER_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    STREET, 
    CITY, 
    STATE, 
    COUNTRY,
    update_timestamp AS start_time,
    '9999-12-31'::TIMESTAMP_NTZ AS end_time,
    TRUE AS is_current,
    'I' AS dml_type
FROM customer_table_changes
WHERE metadata$action = 'INSERT'
AND metadata$isupdate = TRUE
UNION
SELECT 
    CUSTOMER_ID, 
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    start_time,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS end_time,
    FALSE AS is_current,
    'U' AS dml_type
FROM customer_history
WHERE customer_id IN (
    SELECT DISTINCT customer_id 
    FROM customer_table_changes
    WHERE metadata$action = 'DELETE'
    AND metadata$isupdate = TRUE
)
AND is_current = TRUE
UNION
-- Handle DELETEs
SELECT 
    ctc.CUSTOMER_ID, 
    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    ch.start_time,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS end_time,
    FALSE AS is_current,
    'D' AS dml_type
FROM customer_history ch
INNER JOIN customer_table_changes ctc
    ON ch.customer_id = ctc.customer_id
WHERE ctc.metadata$action = 'DELETE'
AND ctc.metadata$isupdate = FALSE
AND ch.is_current = TRUE;

CREATE OR REPLACE PROCEDURE pdr_scd4_history()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var cmd = `
        MERGE INTO customer_history ch
        USING v_customer_change_data_scd4 ccd
            ON ch.customer_id = ccd.customer_id
            AND ch.start_time = ccd.start_time
        WHEN MATCHED AND ccd.dml_type = 'U' THEN UPDATE
            SET ch.end_time = ccd.end_time,
                ch.is_current = FALSE
        WHEN MATCHED AND ccd.dml_type = 'D' THEN UPDATE
            SET ch.end_time = ccd.end_time,
                ch.is_current = FALSE
        WHEN NOT MATCHED AND ccd.dml_type = 'I' THEN INSERT
            (customer_id, first_name, last_name, email, street, city, state, country, start_time, end_time, is_current)
            VALUES (ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email, ccd.street, ccd.city, ccd.state, ccd.country, ccd.start_time, ccd.end_time, ccd.is_current);
    `;
    var sql = snowflake.createStatement({sqlText: cmd});
    var result = sql.execute();
    return cmd;
$$;

-- Task for updating the current customer table
CREATE OR REPLACE TASK tsk_scd4_current
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
    CALL pdr_scd4_current();

-- Task for updating the history table
CREATE OR REPLACE TASK tsk_scd4_history
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
    CALL pdr_scd4_history();

-- Suspend tasks initially
ALTER TASK tsk_scd4_current SUSPEND;
ALTER TASK tsk_scd4_history SUSPEND;

SHOW TASKS;

INSERT INTO customer_raw VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '123 Main St', 'New York', 'NY', 'USA');

CALL pdr_scd4_current();
SELECT * FROM customer WHERE customer_id = 1;
CALL pdr_scd4_history();
INSERT INTO customer_raw VALUES
(1, 'John', 'Doe', 'john.doe2@example.com', '123 Main St', 'New York', 'NY', 'USA');
SELECT * FROM customer WHERE customer_id = 1;
SELECT * FROM customer_history WHERE customer_id = 1;

SELECT 
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) AS next_run, 
    SCHEDULED_TIME, 
    CURRENT_TIMESTAMP, 
    NAME, 
    STATE 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE STATE = 'SCHEDULED' 
ORDER BY COMPLETED_TIME DESC;

-------------------------------------------------------------------------------------------