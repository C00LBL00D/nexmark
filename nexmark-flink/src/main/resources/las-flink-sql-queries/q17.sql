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
    `day` VARCHAR,
    total_bids BIGINT,
    rank1_bids BIGINT,
    rank2_bids BIGINT,
    rank3_bids BIGINT,
    min_price BIGINT,
    max_price BIGINT,
    avg_price BIGINT,
    sum_price BIGINT
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
    auction,
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    count(*) AS total_bids,
    count(*)
                                           filter(where price < 10000) AS rank1_bids,
        count(*)
            filter(where price >= 10000 
                and price < 1000000) AS rank2_bids,
        count(*)
            filter(where price >= 1000000) AS rank3_bids,
        min(price) AS min_price,
    max(price) AS max_price,
    avg(price) AS avg_price,
    sum(price) AS sum_price
FROM
    bid
GROUP BY
    auction, DATE_FORMAT(dateTime, 'yyyy-MM-dd');