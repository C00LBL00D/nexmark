CREATE TEMPORARY TABLE nexmark_table (
    event_type INT,
    person ROW < id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR, city VARCHAR, state VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,
    auction ROW < id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT, dateTime TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra VARCHAR >,
    bid ROW < auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, dateTime TIMESTAMP(3), extra VARCHAR >,
    dateTime AS CASE
        WHEN event_type = 0
        THEN person.dateTime
        WHEN event_type = 1
        THEN auction.dateTime
        ELSE bid.dateTime
    END,
    WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND
)
WITH (
    'connector' = 'nexmark',
    'first-event.rate' = '55000',
    'next-event.rate' = '55000',
    'events.num' = '100000000',
    'person.proportion' = '2',
    'auction.proportion' = '6',
    'bid.proportion' = '92'
);

CREATE TEMPORARY TABLE discard_sink (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    dateTime TIMESTAMP(3),
    extra VARCHAR
)
WITH ('connector' = 'blackhole');

CREATE TEMPORARY VIEW bid AS
SELECT
    bid.auction,
    bid.bidder,
    bid.price,
    bid.channel,
    bid.url,
    dateTime,
    bid.extra
FROM
    nexmark_table
WHERE
        event_type = 2;

INSERT INTO discard_sink
SELECT
    B.auction, B.price, B.bidder, B.dateTime, B.extra
from
    bid B
        JOIN (
        SELECT
            MAX(B1.price) AS maxprice,
            TUMBLE_ROWTIME(B1.dateTime, INTERVAL '10' SECOND) as dateTime
        FROM
            bid B1
        GROUP BY
            TUMBLE(B1.dateTime, INTERVAL '10' SECOND)
    ) B1
             ON B.price = B1.maxprice
WHERE
    B.dateTime BETWEEN B1.dateTime - INTERVAL '10' SECOND AND B1.dateTime;
