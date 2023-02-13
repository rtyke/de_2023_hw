-- Question 1
SELECT count(*) FROM `taxi-rides-ny-375713.zoomcamp.fhv_internal`;

-- Question 3
SELECT count(*) FROM `taxi-rides-ny-375713.zoomcamp.fhv_internal`
where PUlocationID is NULL and DOlocationID is NULL;

-- Question 4
CREATE OR REPLACE TABLE `zoomcamp.fhv_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `zoomcamp.fhv_internal`
);

-- Question 4
SELECT distinct(affiliated_base_number) FROM  `zoomcamp.fhv_internal`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

SELECT distinct(affiliated_base_number) FROM  `zoomcamp.fhv_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';