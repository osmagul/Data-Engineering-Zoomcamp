# 0
```CREATE OR REPLACE EXTERNAL TABLE
`dtc-de-376110.fhv.fhv_2019`
OPTIONS  (
  format='CSV',
  uris=['gs://dtc_data_lake_dtc-de-376110/fhv_2019/fhv_tripdata_2019-*.csv.gz']
);
```

```CREATE OR REPLACE TABLE
`dtc-de-376110.fhv.fhv_2019_materialized` AS(
SELECT * FROM `dtc-de-376110.fhv.fhv_2019`
);
```

# 1

```
SELECT count(*) FROM `dtc-de-376110.fhv.fhv_2019_materialized`
```

# 2
```
SELECT distinct Affiliated_base_number FROM `dtc-de-376110.fhv.fhv_2019_materialized`;
SELECT distinct Affiliated_base_number FROM `dtc-de-376110.fhv.fhv_2019`;
```
# 3
```
SELECT count(*) FROM `dtc-de-376110.fhv.fhv_2019_materialized`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```
# 4
* Partition for filtering, because process less data
* Cluster for ordering, because it means how rows are ordered inside of partitions

# 5
```
SELECT distinct affiliated_base_number FROM `dtc-de-376110.fhv.fhv_2019_materialized`
WHERE pickup_datetime between '2019-03-01' and '2019-03-31'
```

```
CREATE OR REPLACE TABLE `dtc-de-376110.fhv.fhv_2019_partitioned`
PARTITION BY 
  DATE(pickup_datetime) AS 
SELECT * FROM `dtc-de-376110.fhv.fhv_2019_materialized`;
```

```
SELECT distinct affiliated_base_number FROM `dtc-de-376110.fhv.fhv_2019_partitioned`
WHERE pickup_datetime between '2019-03-01' and '2019-03-31'
```

# 6
External table is stored in GCP Bucket

# 7
* Best practice is using BigQuery’s table partitioning and clustering features to structure your data to match common data access patterns. However, tables with size <1gb don't show significant improvement with both partitioning and clustering. So, answer is False.