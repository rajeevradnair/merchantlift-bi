## Databricks Execution Notes

The SCD dimension build should run both locally and in Databricks.

Local execution proves the code works.

Databricks execution proves the same Spark/Delta logic works in a lakehouse runtime.



## Required Input Tables

The SCD job expects these Silver Delta tables to already exist:

```text
data/lakehouse/silver/dim_merchant_clean
data/lakehouse/silver/dim_offer_clean
data/lakehouse/silver/dim_campaign_clean
```

The SCD job writes:

```text
/dbfs/FileStore/merchantlift/data/lakehouse/silver/dim_merchant_scd
/dbfs/FileStore/merchantlift/data/lakehouse/silver/dim_offer_scd
/dbfs/FileStore/merchantlift/data/lakehouse/silver/dim_campaign_scd
```

## Recommended Databricks Run Order

1. Confirm raw data exists.
2. Run Bronze ingestion.
3. Run Silver transformations.
4. Run redemption matching if needed.
5. Run SCD dimension build.
6. Inspect SCD outputs.



## Output of build_scd_dimensions.py in Databricks 

Starting SCD dimension build
================================================================================
Silver directory: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver
Pipeline run ID: scd_run_20260608_054159
SCD rule version: scd_rules_v1
Configured dimensions: 3
================================================================================

Configured SCD: dim_merchant_clean -> dim_merchant_scd

================================================================================
Inspecting Silver dimension source: dim_merchant_clean
================================================================================

Schema:
root
 |-- merchant_id: string (nullable = true)
 |-- merchant_name: string (nullable = true)
 |-- category_id: string (nullable = true)
 |-- location_id: string (nullable = true)
 |-- merchant_status: string (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- merchant_start_date: date (nullable = true)
 |-- created_at: timestamp_ntz (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_table_name: string (nullable = true)
 |-- source_file_path: string (nullable = true)
 |-- pipeline_run_id: string (nullable = true)
 |-- record_hash: string (nullable = true)
 |-- silver_transformed_at: timestamp (nullable = true)
 |-- silver_pipeline_run_id: string (nullable = true)
 |-- quality_status: string (nullable = true)
 |-- validation_rule_version: string (nullable = true)


Row count:
dim_merchant_clean: 200 rows

Sample rows:
+---------------+------------------------+---------------+-------------+---------------+--------------------+-----------------+-------------------+--------------------------+-------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|merchant_id    |merchant_name           |category_id    |location_id  |merchant_status|merchant_margin_rate|platform_fee_rate|merchant_start_date|created_at                |ingestion_timestamp      |source_table_name|source_file_path                                                              |pipeline_run_id           |record_hash                                                     |silver_transformed_at     |silver_pipeline_run_id    |quality_status|validation_rule_version|
+---------------+------------------------+---------------+-------------+---------------+--------------------+-----------------+-------------------+--------------------------+-------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|merchant_000027|Davis Group             |category_000005|location_0020|active         |0.5423              |0.0235           |2023-02-27         |2026-06-03 00:58:16.48084 |2026-06-07 19:47:06.29694|dim_merchant     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_merchant|bronze_run_20260607_194640|6dac55e992422854989ebe67e60c13a189b9e8e8d63dd9093a6a12d17dbd175f|2026-06-07 23:52:45.902584|silver_run_20260607_235211|passed        |silver_rules_v1        |
|merchant_000033|Kidd, Huff and Novak    |category_000002|location_0018|inactive       |0.1132              |0.0243           |2025-12-21         |2026-06-03 00:58:16.481342|2026-06-07 19:47:06.29694|dim_merchant     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_merchant|bronze_run_20260607_194640|294a0eab1e99dd817bf5fe2e70ee64d4a496962d8f1241c7237d9bcdc3e015b5|2026-06-07 23:52:45.902584|silver_run_20260607_235211|passed        |silver_rules_v1        |
|merchant_000054|Campbell-Clark          |category_000003|location_0005|active         |0.4933              |0.032            |2023-02-07         |2026-06-03 00:58:16.483156|2026-06-07 19:47:06.29694|dim_merchant     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_merchant|bronze_run_20260607_194640|3214c1311b9739fbb0346cb8f2734403f64c6c45efe2791b13f11ffb8ebb8f3e|2026-06-07 23:52:45.902584|silver_run_20260607_235211|passed        |silver_rules_v1        |
|merchant_000056|Davis Group             |category_000005|location_0021|active         |0.5529              |0.0239           |2025-06-30         |2026-06-03 00:58:16.483341|2026-06-07 19:47:06.29694|dim_merchant     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_merchant|bronze_run_20260607_194640|4b9f7b0daf5824344a13975b44cc7542dea5deaa589b3f230d458ba343124d4e|2026-06-07 23:52:45.902584|silver_run_20260607_235211|passed        |silver_rules_v1        |
|merchant_000076|Lee, Williams and Graham|category_000002|location_0019|active         |0.1963              |0.0127           |2023-12-22         |2026-06-03 00:58:16.485461|2026-06-07 19:47:06.29694|dim_merchant     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_merchant|bronze_run_20260607_194640|620a61ecc0b16effa6cb54daf7ea7ebd043589f445158f254505648ab6a54a51|2026-06-07 23:52:45.902584|silver_run_20260607_235211|passed        |silver_rules_v1        |
+---------------+------------------------+---------------+-------------+---------------+--------------------+-----------------+-------------------+--------------------------+-------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
only showing top 5 rows

Configured SCD: dim_offer_clean -> dim_offer_scd

================================================================================
Inspecting Silver dimension source: dim_offer_clean
================================================================================

Schema:
root
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- offer_name: string (nullable = true)
 |-- offer_type: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- reward_multiplier: double (nullable = true)
 |-- max_reward_amount: double (nullable = true)
 |-- offer_start_date: date (nullable = true)
 |-- offer_end_date: date (nullable = true)
 |-- offer_status: string (nullable = true)
 |-- created_at: timestamp_ntz (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_table_name: string (nullable = true)
 |-- source_file_path: string (nullable = true)
 |-- pipeline_run_id: string (nullable = true)
 |-- record_hash: string (nullable = true)
 |-- silver_transformed_at: timestamp (nullable = true)
 |-- silver_pipeline_run_id: string (nullable = true)
 |-- quality_status: string (nullable = true)
 |-- validation_rule_version: string (nullable = true)


Row count:
dim_offer_clean: 300 rows

Sample rows:
+------------+---------------+---------------+--------------------------+----------------+--------------------+-------------+-----------------+-----------------+----------------+--------------+------------+--------------------------+--------------------------+-----------------+---------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|offer_id    |campaign_id    |merchant_id    |offer_name                |offer_type      |minimum_spend_amount|reward_amount|reward_multiplier|max_reward_amount|offer_start_date|offer_end_date|offer_status|created_at                |ingestion_timestamp       |source_table_name|source_file_path                                                           |pipeline_run_id           |record_hash                                                     |silver_transformed_at     |silver_pipeline_run_id    |quality_status|validation_rule_version|
+------------+---------------+---------------+--------------------------+----------------+--------------------+-------------+-----------------+-----------------+----------------+--------------+------------+--------------------------+--------------------------+-----------------+---------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|offer_000217|campaign_000055|merchant_000052|Fixed Cashback Offer 217  |fixed_cashback  |100.0               |10.0         |0.0              |10.0             |2026-02-22      |2026-03-24    |expired     |2026-06-03 00:58:16.50387 |2026-06-07 19:47:12.821787|dim_offer        |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_offer|bronze_run_20260607_194640|d5839b7464e6a6d1647dc436ae476d905783cf19c8cbd5010085473b6457c2db|2026-06-07 23:53:01.609724|silver_run_20260607_235211|passed        |silver_rules_v1        |
|offer_000222|campaign_000055|merchant_000178|Fixed Cashback Offer 222  |fixed_cashback  |150.0               |5.0          |0.0              |5.0              |2026-02-22      |2026-03-24    |active      |2026-06-03 00:58:16.50388 |2026-06-07 19:47:12.821787|dim_offer        |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_offer|bronze_run_20260607_194640|d3fdbe40a66dd9e1e65c969eaf8c91cf8b4bd71e53873f2f0da9da987c9ddd02|2026-06-07 23:53:01.609724|silver_run_20260607_235211|passed        |silver_rules_v1        |
|offer_000189|campaign_000055|merchant_000100|Fixed Cashback Offer 189  |fixed_cashback  |75.0                |10.0         |0.0              |10.0             |2026-02-22      |2026-03-24    |active      |2026-06-03 00:58:16.503815|2026-06-07 19:47:12.821787|dim_offer        |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_offer|bronze_run_20260607_194640|ae2c1b496030a148d12b2bae558006bd27e6ed7bd4dae5602739a9b215b8e5c8|2026-06-07 23:53:01.609724|silver_run_20260607_235211|passed        |silver_rules_v1        |
|offer_000214|campaign_000055|merchant_000055|Percent Cashback Offer 214|percent_cashback|150.0               |0.0          |0.15             |75.0             |2026-02-22      |2026-03-24    |active      |2026-06-03 00:58:16.503864|2026-06-07 19:47:12.821787|dim_offer        |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_offer|bronze_run_20260607_194640|b5fa0b7a739e7b9c292663ae7431d3d1f3cd16f157fc4ca6d77c402df49f7748|2026-06-07 23:53:01.609724|silver_run_20260607_235211|passed        |silver_rules_v1        |
|offer_000204|campaign_000055|merchant_000027|Fixed Cashback Offer 204  |fixed_cashback  |100.0               |5.0          |0.0              |5.0              |2026-02-22      |2026-03-24    |active      |2026-06-03 00:58:16.503843|2026-06-07 19:47:12.821787|dim_offer        |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_offer|bronze_run_20260607_194640|ee3ee3faf643351ce54bb14078921a54437cc6afce52f33ca2499e6d2b2ae2d9|2026-06-07 23:53:01.609724|silver_run_20260607_235211|passed        |silver_rules_v1        |
+------------+---------------+---------------+--------------------------+----------------+--------------------+-------------+-----------------+-----------------+----------------+--------------+------------+--------------------------+--------------------------+-----------------+---------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
only showing top 5 rows

Configured SCD: dim_campaign_clean -> dim_campaign_scd

================================================================================
Inspecting Silver dimension source: dim_campaign_clean
================================================================================

Schema:
root
 |-- campaign_id: string (nullable = true)
 |-- campaign_name: string (nullable = true)
 |-- campaign_objective: string (nullable = true)
 |-- campaign_start_date: date (nullable = true)
 |-- campaign_end_date: date (nullable = true)
 |-- campaign_status: string (nullable = true)
 |-- target_segment_id: string (nullable = true)
 |-- budget_amount: double (nullable = true)
 |-- created_at: timestamp_ntz (nullable = true)
 |-- ingestion_timestamp: timestamp (nullable = true)
 |-- source_table_name: string (nullable = true)
 |-- source_file_path: string (nullable = true)
 |-- pipeline_run_id: string (nullable = true)
 |-- record_hash: string (nullable = true)
 |-- silver_transformed_at: timestamp (nullable = true)
 |-- silver_pipeline_run_id: string (nullable = true)
 |-- quality_status: string (nullable = true)
 |-- validation_rule_version: string (nullable = true)


Row count:
dim_campaign_clean: 80 rows

Sample rows:
+---------------+------------------------------+------------------+-------------------+-----------------+---------------+-----------------+-------------+--------------------------+--------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|campaign_id    |campaign_name                 |campaign_objective|campaign_start_date|campaign_end_date|campaign_status|target_segment_id|budget_amount|created_at                |ingestion_timestamp       |source_table_name|source_file_path                                                              |pipeline_run_id           |record_hash                                                     |silver_transformed_at     |silver_pipeline_run_id    |quality_status|validation_rule_version|
+---------------+------------------------------+------------------+-------------------+-----------------+---------------+-----------------+-------------+--------------------------+--------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
|campaign_000056|Basket Size Growth Campaign 56|basket_size_growth|2026-01-31         |2026-02-14       |completed      |segment_002      |86363.21     |2026-06-03 00:58:16.500733|2026-06-07 19:47:09.459812|dim_campaign     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_campaign|bronze_run_20260607_194640|6284b1f0c039730cbf45aed8e92511f52720d78a4c5de700938827938cede290|2026-06-07 23:52:51.599555|silver_run_20260607_235211|passed        |silver_rules_v1        |
|campaign_000005|Category Share Campaign 5     |category_share    |2026-03-15         |2026-04-05       |completed      |segment_002      |327393.49    |2026-06-03 00:58:16.500582|2026-06-07 19:47:09.459812|dim_campaign     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_campaign|bronze_run_20260607_194640|2101233a2b60deaecb7c88893c671619394f0d3314c588836b545040f6eaca93|2026-06-07 23:52:51.599555|silver_run_20260607_235211|passed        |silver_rules_v1        |
|campaign_000028|Repeat Purchase Campaign 28   |repeat_purchase   |2026-01-05         |2026-01-26       |active         |segment_006      |224956.26    |2026-06-03 00:58:16.500654|2026-06-07 19:47:09.459812|dim_campaign     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_campaign|bronze_run_20260607_194640|121136093191b71c4a442bd76bbe1fe358220b932847988fe933cf77b17b1a89|2026-06-07 23:52:51.599555|silver_run_20260607_235211|passed        |silver_rules_v1        |
|campaign_000055|Reactivation Campaign 55      |reactivation      |2026-02-22         |2026-03-24       |completed      |segment_003      |162487.39    |2026-06-03 00:58:16.50073 |2026-06-07 19:47:09.459812|dim_campaign     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_campaign|bronze_run_20260607_194640|cc22b47cde5df7de0e4fabd6701f3dc4a3932b786fd5af73a4234d19846ae8f5|2026-06-07 23:52:51.599555|silver_run_20260607_235211|passed        |silver_rules_v1        |
|campaign_000004|Acquisition Campaign 4        |acquisition       |2026-01-03         |2026-03-04       |completed      |segment_006      |260945.46    |2026-06-03 00:58:16.500578|2026-06-07 19:47:09.459812|dim_campaign     |/Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/raw/dim_campaign|bronze_run_20260607_194640|f08e6fb09be8d661c08c3a539a84d9e5a0d2c3b6792990435006b5677591754b|2026-06-07 23:52:51.599555|silver_run_20260607_235211|passed        |silver_rules_v1        |
+---------------+------------------------------+------------------+-------------------+-----------------+---------------+-----------------+-------------+--------------------------+--------------------------+-----------------+------------------------------------------------------------------------------+--------------------------+----------------------------------------------------------------+--------------------------+--------------------------+--------------+-----------------------+
only showing top 5 rows

SCD source dimension summary
================================================================================
source_table                                     rows
--------------------------------------------------------------------------------
dim_merchant_clean                                200
dim_offer_clean                                   300
dim_campaign_clean                                 80
--------------------------------------------------------------------------------
TOTAL                                             580
================================================================================

================================================================================
Building SCD dimension: dim_merchant_scd
Source table: dim_merchant_clean
Business key: merchant_id
Tracked columns: merchant_name, category_id, location_id, merchant_margin_rate, platform_fee_rate
================================================================================

SCD build result
================================================================================
source rows                                       200
scd rows                                          200
rows removed                                        0
================================================================================

SCD schema
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- merchant_name: string (nullable = true)
 |-- category_id: string (nullable = true)
 |-- location_id: string (nullable = true)
 |-- merchant_margin_rate: double (nullable = true)
 |-- platform_fee_rate: double (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = false)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = false)
 |-- scd_updated_at: timestamp (nullable = false)
 |-- scd_pipeline_run_id: string (nullable = false)
 |-- scd_rule_version: string (nullable = false)


SCD sample
+--------------------------------------------------------------------+---------------+-------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |merchant_id    |merchant_name            |category_id    |location_id  |merchant_margin_rate|platform_fee_rate|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at            |scd_updated_at            |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+---------------+-------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|scd_51f611478722083c7167cd3240691d47c77fb2da5472f30a26bfb64e112cabb9|merchant_000027|Davis Group              |category_000005|location_0020|0.5423              |0.0235           |2026-01-01          |NULL              |true      |eeaf0af7b6fab83c34fd623542f7ad88d4ebf65a8dee28c887f3e4db735354d1|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_9fa4a44aeb36e94aab88bff19a6d4af3a37e85f9dde133eed215b91fd84eb718|merchant_000033|Kidd, Huff and Novak     |category_000002|location_0018|0.1132              |0.0243           |2026-01-01          |NULL              |true      |f3d45a905300a3ecc5bccb443a4db4e55e80d43f3289fa852d94bb149963178f|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_9802cf5d207396460fa83469e72a2fbcdd12620abcb19c35ecc416b54a9ee65f|merchant_000054|Campbell-Clark           |category_000003|location_0005|0.4933              |0.032            |2026-01-01          |NULL              |true      |8aa1ec958bf17289ec1d721d6352978ebc43d90c3910f0070ff184e693347cb8|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_72e0259a6e5b37eb4cd5f967c29568ee5cdec46880117344b805209ad3ee5d74|merchant_000056|Davis Group              |category_000005|location_0021|0.5529              |0.0239           |2026-01-01          |NULL              |true      |d207987b007ab17e9ee85970f32ecd514208e33a6585ed07dd9cb3cd7fac42d3|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_ddd3e4536be7a4a8715192703573b788b3d1956865a213efe752061db37ec059|merchant_000076|Lee, Williams and Graham |category_000002|location_0019|0.1963              |0.0127           |2026-01-01          |NULL              |true      |59b3c67ad7ab333222f52452fea44c4f889569b7c9b9ea9dcc8559cddacbafa4|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_874f3d3281d8d7e49dea51e67beb6915062ef3f5e13903b4b47826f362301768|merchant_000106|Bender LLC               |category_000003|location_0025|0.439               |0.0309           |2026-01-01          |NULL              |true      |eba7b1b4a045e1d73d8eb955c41aee8c2738a21265fa9cffca3c02666d572469|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_f9d55f8790415cf726a3feb6d3f814a96330c2996f4aaa41e8d129fadcded35e|merchant_000120|Obrien Ltd               |category_000003|location_0004|0.4579              |0.0244           |2026-01-01          |NULL              |true      |2b5255b78604330b1ce9f0e14d47fa9a6a7b7f813c3c3e577492cb67cbc9bd5d|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_8a0b6bb21ef9f0b7f5bf504b44d2c99e7288a480b8e4e6dca438f2b77d707f1a|merchant_000146|Tran Ltd                 |category_000001|location_0013|0.3492              |0.0272           |2026-01-01          |NULL              |true      |df4ea76b1a748e5c70ec576773c86afcc97ac1e9e54aea42993d2136a9e94d89|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_359b3baa307e6f5af0508ca07df378badc05c8ae0be65a195b70fa3f86c164d1|merchant_000176|Sandoval PLC             |category_000006|location_0010|0.7009              |0.0309           |2026-01-01          |NULL              |true      |5eb9f93798175ccfa3ea3137f07bd4e91913e5ba1919a799e8989e4731ad3556|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
|scd_65276ac9c61c46c52c459b5923664fe52a2ddb489b9a883f3dd7e5eb223306fc|merchant_000184|Hill, Davenport and Baird|category_000001|location_0011|0.3745              |0.0303           |2026-01-01          |NULL              |true      |770da1282f2efa834fe37d6dceeb144f1d7502d748f0dde84407176a41e625d1|2026-06-08 05:42:06.685358|2026-06-08 05:42:06.685358|scd_run_20260608_054159|scd_rules_v1    |
+--------------------------------------------------------------------+---------------+-------------------------+---------------+-------------+--------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
only showing top 10 rows

================================================================================
Building SCD dimension: dim_offer_scd
Source table: dim_offer_clean
Business key: offer_id
Tracked columns: campaign_id, merchant_id, minimum_spend_amount, reward_amount, offer_start_date, offer_end_date
================================================================================

SCD build result
================================================================================
source rows                                       300
scd rows                                          300
rows removed                                        0
================================================================================

SCD schema
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- offer_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- merchant_id: string (nullable = true)
 |-- minimum_spend_amount: double (nullable = true)
 |-- reward_amount: double (nullable = true)
 |-- offer_start_date: date (nullable = true)
 |-- offer_end_date: date (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = false)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = false)
 |-- scd_updated_at: timestamp (nullable = false)
 |-- scd_pipeline_run_id: string (nullable = false)
 |-- scd_rule_version: string (nullable = false)


SCD sample
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+-------------------------+-------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |offer_id    |campaign_id    |merchant_id    |minimum_spend_amount|reward_amount|offer_start_date|offer_end_date|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at           |scd_updated_at           |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+-------------------------+-------------------------+-----------------------+----------------+
|scd_f9644844a30a76dc0ccec7a38ad4ac5e01270089a4c07c37f56fd8c0d2bd00d9|offer_000069|campaign_000051|merchant_000096|150.0               |0.0          |2026-06-08      |2026-06-22    |2026-01-01          |NULL              |true      |55afb1a3d43b847f538d651ff21e50158f4f2030aec546cdb17af46e596351c5|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_5d76f6b8cc7164138551d9f7b596c1d3dd86ae51898a42af310b344be173deb9|offer_000269|campaign_000012|merchant_000085|50.0                |20.0         |2026-04-12      |2026-04-26    |2026-01-01          |NULL              |true      |dc0f0697d9ce9674ae20333cd922300bc7eb79453e97cfa97024961565921729|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_13fce9f9569d2d0d6d986433c25492ec0fc3d133cf528a2a10d95f1da3d9272a|offer_000042|campaign_000041|merchant_000171|50.0                |0.0          |2026-05-24      |2026-06-23    |2026-01-01          |NULL              |true      |16b2def88ff8fb3d13d93a500b79494fcadb09af1b83bcece0032b9ae6356c42|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_604e3a7ab7e666176481b69ffcc585c3dc780fecb8e6a1c8fc17b1077967e505|offer_000268|campaign_000010|merchant_000006|75.0                |20.0         |2026-05-04      |2026-06-03    |2026-01-01          |NULL              |true      |b9136f6ebe792a135ae5dea51aeb88c24a1bdb7e8b2fc084f8cf1960f60c10df|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_1eaea75594f6361c1771619c3e318a717794e396928c8357ab256e4c0715ca86|offer_000250|campaign_000040|merchant_000084|250.0               |20.0         |2026-01-25      |2026-02-24    |2026-01-01          |NULL              |true      |0b07fb7fa7461914018b125ca1b183efadab801d10979a6cf46dceaa2ea94118|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_158adc3ae7eb3d34febf83346543a2d66e6a7eaaacbe2a4e8e4bed91f5c8d944|offer_000179|campaign_000046|merchant_000043|250.0               |20.0         |2026-06-05      |2026-07-05    |2026-01-01          |NULL              |true      |31767acd8f704a4f62d65f97bb207e8b1725df123960f25de891c4a40949729c|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_029d2f1943ead267738a93374d480f2dc9da13c3b7e1e952beaccefd2afb51d2|offer_000142|campaign_000010|merchant_000172|50.0                |20.0         |2026-05-04      |2026-06-03    |2026-01-01          |NULL              |true      |80dd5bca8faa9f4116f95e6c1661a63b04eb96b6093e694897629f54d40ce2f6|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_596f754c9943b1cd9bed28965899f7555f09bc660af28cc2a469a9ba74b6ea33|offer_000084|campaign_000070|merchant_000098|150.0               |0.0          |2026-03-21      |2026-05-05    |2026-01-01          |NULL              |true      |388e0584f7edaf0f110377fdb2bb3ca2c9c0482847bd43b6decc0aa171f156ea|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_a6ead52f03c1bd0c87fa3e3a6209f32b95c9d87581c574bdb094e8dd80c151d9|offer_000063|campaign_000040|merchant_000128|50.0                |20.0         |2026-01-25      |2026-02-24    |2026-01-01          |NULL              |true      |a7327a3ccfbfd3a0197ce43de65f3ee0fa05b4b76fc67d6069b618a46ef2ec84|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
|scd_dd47bc936bc2dd5e8c953ff2ca879152a717e2d97459629b2f70dab0dc25ead4|offer_000175|campaign_000038|merchant_000178|150.0               |20.0         |2026-01-25      |2026-02-24    |2026-01-01          |NULL              |true      |0a200c04dc9f3c6ebc74522c9b14fdecabf777282f6c8006ac2e4d68b40c2560|2026-06-08 05:42:09.60587|2026-06-08 05:42:09.60587|scd_run_20260608_054159|scd_rules_v1    |
+--------------------------------------------------------------------+------------+---------------+---------------+--------------------+-------------+----------------+--------------+--------------------+------------------+----------+----------------------------------------------------------------+-------------------------+-------------------------+-----------------------+----------------+
only showing top 10 rows

================================================================================
Building SCD dimension: dim_campaign_scd
Source table: dim_campaign_clean
Business key: campaign_id
Tracked columns: campaign_name, campaign_start_date, campaign_end_date
================================================================================

SCD build result
================================================================================
source rows                                        80
scd rows                                           80
rows removed                                        0
================================================================================

SCD schema
root
 |-- surrogate_scd_id: string (nullable = true)
 |-- campaign_id: string (nullable = true)
 |-- campaign_name: string (nullable = true)
 |-- campaign_start_date: date (nullable = true)
 |-- campaign_end_date: date (nullable = true)
 |-- effective_start_date: date (nullable = true)
 |-- effective_end_date: date (nullable = true)
 |-- is_current: boolean (nullable = false)
 |-- scd_record_hash: string (nullable = true)
 |-- scd_created_at: timestamp (nullable = false)
 |-- scd_updated_at: timestamp (nullable = false)
 |-- scd_pipeline_run_id: string (nullable = false)
 |-- scd_rule_version: string (nullable = false)


SCD sample
+--------------------------------------------------------------------+---------------+------------------------------+-------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|surrogate_scd_id                                                    |campaign_id    |campaign_name                 |campaign_start_date|campaign_end_date|effective_start_date|effective_end_date|is_current|scd_record_hash                                                 |scd_created_at            |scd_updated_at            |scd_pipeline_run_id    |scd_rule_version|
+--------------------------------------------------------------------+---------------+------------------------------+-------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
|scd_eb17a82d3b2f8d85bf54302ff67ab094e9cdc93e1d8835bf9edeec993ad7b02e|campaign_000022|Reactivation Campaign 22      |2026-04-12         |2026-05-12       |2026-01-01          |NULL              |true      |182dd6562e6ee6bc1ab1cb5fd4f9735f144e1310e04d1054541be3fe4111b85d|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_1524d0c5f6a612d27fd9721b490a664ccfdd41101046f7b7e9ff4cd4897f8397|campaign_000052|Basket Size Growth Campaign 52|2026-06-05         |2026-07-20       |2026-01-01          |NULL              |true      |f3e69b5869414009b76c1070a9260385e0dd0c1bffc82ae894b1bfcf8883b378|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_6c454baf6480c029cceb49d6a6b2fbf25f8aba3c7e1a6ad74895e331dd788db7|campaign_000003|Acquisition Campaign 3        |2026-03-17         |2026-04-16       |2026-01-01          |NULL              |true      |89308b33982190399410015767381e024deedf2b0c9f56948aaf3c54b4193294|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_02df6964a808e09756dc2e0cd4d25e80d8a44f258d3676a282bbd50063adf9d5|campaign_000008|Basket Size Growth Campaign 8 |2026-03-17         |2026-04-16       |2026-01-01          |NULL              |true      |7c3db1a4e6644c76d41e32d9ee3c0bb91147a4eae6273249e9f0743482c3b30c|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_588aacdf060d7ccf4b29d772491cb49aa59be6b7bad79eaea2554d743b87aceb|campaign_000046|Acquisition Campaign 46       |2026-06-05         |2026-07-05       |2026-01-01          |NULL              |true      |1ea0f20a33b08aa434c72c20ef1c6d6b81639958448b319a26ede6025f9edfe3|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_5513c02e8faa051ca7950edab9b231d3aba775535c2b13eb643c44584d678c21|campaign_000030|Category Share Campaign 30    |2026-01-28         |2026-03-14       |2026-01-01          |NULL              |true      |ef5b6638acafe2942237c6ada1821c98b0b82d51e874f35e46ba8bc9cc0a8ddb|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_9793de559ce7bf038fc316d74044dbff6c9b821cbc0b177f09f8085524176d86|campaign_000006|Reactivation Campaign 6       |2026-06-08         |2026-07-08       |2026-01-01          |NULL              |true      |bde8dae7dc9be7dc193e3db499daa33df5826fd6c31c9249bfa6766883bcb327|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_6666ad5ab22c3c3e188d9005ff798eed5960d8fc9e1718ff72e3d873b14e9ab0|campaign_000076|Basket Size Growth Campaign 76|2026-03-14         |2026-04-28       |2026-01-01          |NULL              |true      |dc2dff95cf2a3b871444a5b80b49c6e8f1656f14eb286e9a5fbf9ebab69b5d4b|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_b4d5a0f454607c0588808b0b252c5144141c0e9b66e156c92f37d5f2026d88c5|campaign_000040|Reactivation Campaign 40      |2026-01-25         |2026-02-24       |2026-01-01          |NULL              |true      |c137decead26aea0535d88389e48d2363e89f530d4bd5ce89de8dd2826eb1c06|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
|scd_69b9e42d1fdc419a67e18b4f2ed211ac31225f2205f6d03130e413a29936c20a|campaign_000050|Category Share Campaign 50    |2026-03-21         |2026-04-20       |2026-01-01          |NULL              |true      |df8f7add13a0bbbc8a15b868dcb2ce7181df0a7d344a49b8928b9dc760efaabf|2026-06-08 05:42:12.443748|2026-06-08 05:42:12.443748|scd_run_20260608_054159|scd_rules_v1    |
+--------------------------------------------------------------------+---------------+------------------------------+-------------------+-----------------+--------------------+------------------+----------+----------------------------------------------------------------+--------------------------+--------------------------+-----------------------+----------------+
only showing top 10 rows

SCD dimension build summary
================================================================================
output_table                         source_rows     scd_rows      removed
--------------------------------------------------------------------------------
dim_merchant_scd                             200          200            0
dim_offer_scd                                300          300            0
dim_campaign_scd                              80           80            0
--------------------------------------------------------------------------------
TOTAL                                        580          580            0
================================================================================

Writing SCD Delta tables
================================================================================
Wrote SCD table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver/dim_merchant_scd

Written SCD table validation
================================================================================
table                                    dim_merchant_scd
expected rows                                     200
actual rows                                       200
================================================================================
SCD write validation passed: dim_merchant_scd
Wrote SCD table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver/dim_offer_scd

Written SCD table validation
================================================================================
table                                    dim_offer_scd
expected rows                                     300
actual rows                                       300
================================================================================
SCD write validation passed: dim_offer_scd
Wrote SCD table: /Volumes/workspace/merchantlift/raw_lake/merchantlift-bi/data/lakehouse/silver/dim_campaign_scd

Written SCD table validation
================================================================================
table                                    dim_campaign_scd
expected rows                                      80
actual rows                                        80
================================================================================
SCD write validation passed: dim_campaign_scd

All SCD Delta tables written and validated.

Validating all SCD outputs
================================================================================

SCD business-rule validation
================================================================================
table                                         dim_merchant_scd
null required rows                                       0
invalid effective date rows                              0
current rows with end date                               0
business keys without exactly one current row            0
duplicate surrogate_scd_id values                        0
total validation failures                                0
================================================================================

SCD business-rule validation
================================================================================
table                                         dim_offer_scd
null required rows                                       0
invalid effective date rows                              0
current rows with end date                               0
business keys without exactly one current row            0
duplicate surrogate_scd_id values                        0
total validation failures                                0
================================================================================

SCD business-rule validation
================================================================================
table                                         dim_campaign_scd
null required rows                                       0
invalid effective date rows                              0
current rows with end date                               0
business keys without exactly one current row            0
duplicate surrogate_scd_id values                        0
total validation failures                                0
================================================================================

SCD validation summary
================================================================================
table                                             failures       status
--------------------------------------------------------------------------------
dim_merchant_scd                                         0       PASSED
dim_offer_scd                                            0       PASSED
dim_campaign_scd                                         0       PASSED
================================================================================
All SCD output validations passed.
