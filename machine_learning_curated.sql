CREATE EXTERNAL TABLE IF NOT EXISTS `stediprojecttask_`.`machine_learning_curated` (
  `serialnumber` string,
  `x` double,
  `z` double,
  `y` double,
  `user` string,
  `timestamp` string,
  `sensorreadingtime` string,
  `distancefromobject` int,
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `registrationdate` bigint,
  `lastupdatedate` bigint,
  `sharewithresearchasofdate` bigint,
  `sharewithpublicasofdate` bigint,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-for-udacity-project/curated/'
TBLPROPERTIES ('classification' = 'json');