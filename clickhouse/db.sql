CREATE TABLE raw_url
(
    `sslsni` String,
    `subscriberid` CHAR(15),
    `hour_key` UInt8,
    `count` UInt16,
    `up` UInt64,
    `down` UInt64,
    `inserted_time` DATETIME64
)
ENGINE = MergeTree
PRIMARY KEY (subscriberid, sslsni);

CREATE TABLE top_url
(
    `subscriberid` CHAR(15),
    `sslsni` String,
    `total_up` UInt64,
    `total_down` UInt64,
    `frequent_hour_key_Map` Nested(
        `hour_key` UInt8,
        `count` UInt32
    )
)
ENGINE = SummingMergeTree
PRIMARY KEY (subscriberid, sslsni);

