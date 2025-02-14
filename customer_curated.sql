CREATE EXTERNAL TABLE IF NOT EXISTS `stediprojecttask_`.`customer_curated` (
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialnumber` string,
  `registrationdate` string,
  `lastupdatedate` string,
  `sharewithresearchasofdate` string,
  `sharewithpublicasofdate` string,
  `sharewithfriendsasofdate` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-for-udacity-project/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');