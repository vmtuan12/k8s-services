CREATE TABLE raw
(
    `date` Date,
    `sslsni` String,
    `subscriberid` CHAR(30),
    `hour_key` UInt8,
    `count` UInt16,
    `up` UInt64,
    `down` UInt64
)
ENGINE = MergeTree
PRIMARY KEY (subscriberid, date, sslsni)

CREATE TABLE top_url
(
    `subscriberid` CHAR(30),
    `sslsni` String,
    `total_up` UInt64,
    `total_down` UInt64,
    `frequent_hour_key` Map(UInt8, UInt64),
    `total_access_count` UInt64
)
ENGINE = MergeTree
PRIMARY KEY (subscriberid, sslsni)

