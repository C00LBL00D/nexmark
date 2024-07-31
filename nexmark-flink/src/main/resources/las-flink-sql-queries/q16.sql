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
    channel VARCHAR,
    `day` VARCHAR,
    `minute` VARCHAR,
    total_bids BIGINT,
    rank1_bids BIGINT,
    rank2_bids BIGINT,
    rank3_bids BIGINT,
    total_bidders BIGINT,
    rank1_bidders BIGINT,
    rank2_bidders BIGINT,
    rank3_bidders BIGINT,
    total_auctions BIGINT,
    rank1_auctions BIGINT,
    rank2_auctions BIGINT,
    rank3_auctions BIGINT
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
    channel,
    DATE_FORMAT(dateTime, 'yyyy-MM-dd') as `day`,
    max(DATE_FORMAT(dateTime, 'HH:mm')) as `minute`,
    count(*) AS total_bids,
    count(*)
                                           filter(where price < 10000) AS rank1_bids,
        count(*)
            filter(where price >= 10000 
                and price < 1000000) AS rank2_bids,
        count(*)
            filter(where price >= 1000000) AS rank3_bids,
        count(distinct bidder) AS total_bidders,
    count(distinct bidder)
                                           filter(where price < 10000) AS rank1_bidders,
        count(distinct bidder)
            filter(where price >= 10000 
                and price < 1000000) AS rank2_bidders,
        count(distinct bidder)
            filter(where price >= 1000000) AS rank3_bidders,
        count(distinct auction) AS total_auctions,
    count(distinct auction)
                                           filter(where price < 10000) AS rank1_auctions,
        count(distinct auction)
            filter(where price >= 10000 
                and price < 1000000) AS rank2_auctions,
        count(distinct auction)
            filter(where price >= 1000000) AS rank3_auctions
FROM
    bid
GROUP BY
    channel, DATE_FORMAT(dateTime, 'yyyy-MM-dd');