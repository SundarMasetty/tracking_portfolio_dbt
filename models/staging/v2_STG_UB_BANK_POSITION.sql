-- we will be adding some meta data for this staging version and i will be adding record_source for the previous staging query and a cte
-- RECORD_SOURCE is a metadata which we can able to find the source where it is derived from

--SRC_DATA CTE we will be concerned with the incoming source data we want to adapt and extract the load data that we want
--default_record_cte we will be concerned with the showing the default_records with some meaningfull value
WITH 
    src_data AS (
        SELECT 
            ACCOUNT_ID AS ACCOUNT_CODE,
            SYMBOL AS SECURITY_CODE,
            DESCRIPTION AS SECURITY_NAME,
            EXCHANGE AS EXCHANGE_CODE,
            REPORT_DATE AS REPORT_DATE,
            QUANTITY AS QUANTITY,
            COST_BASE AS COST_BASE,
            POSITION_VALUE AS POSITION_VALUE,
            CURRENCY AS CURRENCY_CODE,
            'SOURCE_DATA.UB_BANK_POSITION' AS RECORD_SOURCE  --Here as we are having the CSV file and we loaded in to table we have hardcoded the value here.
        FROM {{ source('ub_bank', 'UB_BANK_POSITION') }}
    ),


-- Now in this staging table i will be adding the hashed difference because we can track some field changes which can able to capture the data change

hashed AS (
        SELECT 
            concat_ws('|' , ACCOUNT_CODE,SECURITY_CODE) AS POSITION_HKEY
           , concat_ws('|' , ACCOUNT_CODE,SECURITY_CODE,SECURITY_NAME,EXCHANGE_CODE,REPORT_DATE,QUANTITY,COST_BASE,POSITION_VALUE,CURRENCY_CODE ) AS POSITION_HDIFF
            , * 
            , '{{ run_started_at }}' AS LOAD_TS_UTC --metadata loading ot this operation was performed time
            FROM src_data
)
SELECT * FROM hashed


