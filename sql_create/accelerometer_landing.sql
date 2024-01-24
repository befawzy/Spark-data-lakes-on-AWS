CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-bfawzy`.`accelerometer_landing` (
  `timestamp` bigint,
  `user` string,
  `x` float,
  `y` float,
  `z` float
) COMMENT "landing zone for the accelemeter data"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-lake-house-bfawzy/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');