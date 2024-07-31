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

CREATE TEMPORARY TABLE discard_sink (auction BIGINT, num BIGINT)
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
    AuctionBids.auction, AuctionBids.num
FROM
    (
        SELECT
            B1.auction,
            count(*) AS num,
            HOP_START(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
            HOP_END(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
        FROM
            bid B1
        GROUP BY
            B1.auction,
            HOP(B1.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    ) AS AuctionBids
        JOIN (
        SELECT
            max(CountBids.num) AS maxn,
            CountBids.starttime,
            CountBids.endtime
        FROM
            (
                SELECT
                    count(*) AS num,
                    HOP_START(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
                    HOP_END(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
                FROM
                    bid B2
                GROUP BY
                    B2.auction,
                    HOP(B2.dateTime, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
            ) AS CountBids
        GROUP BY
            CountBids.starttime, CountBids.endtime
    ) AS MaxBids
             ON AuctionBids.starttime = MaxBids.starttime
                 AND AuctionBids.endtime = MaxBids.endtime
                 AND AuctionBids.num >= MaxBids.maxn;