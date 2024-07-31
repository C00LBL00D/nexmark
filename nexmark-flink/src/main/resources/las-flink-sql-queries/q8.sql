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

CREATE TEMPORARY TABLE discard_sink (id BIGINT, name VARCHAR, stime TIMESTAMP(3))
WITH ('connector' = 'blackhole');

CREATE TEMPORARY VIEW person AS
SELECT
    person.id,
    person.name,
    person.emailAddress,
    person.creditCard,
    person.city,
    person.state,
    dateTime,
    person.extra
FROM
    nexmark_table
WHERE
        event_type = 0;

CREATE TEMPORARY VIEW auction AS
SELECT
    auction.id,
    auction.itemName,
    auction.description,
    auction.initialBid,
    auction.reserve,
    dateTime,
    auction.expires,
    auction.seller,
    auction.category,
    auction.extra
FROM
    nexmark_table
WHERE
        event_type = 1;

INSERT INTO discard_sink
SELECT
    P.id, P.name, P.starttime
FROM
    (
        SELECT
            P.id,
            P.name,
            TUMBLE_START(P.dateTime, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(P.dateTime, INTERVAL '10' SECOND) AS endtime
        FROM
            person P
        GROUP BY
            P.id,
            P.name,
            TUMBLE(P.dateTime, INTERVAL '10' SECOND)
    ) P
        JOIN (
        SELECT
            A.seller,
            TUMBLE_START(A.dateTime, INTERVAL '10' SECOND) AS starttime,
            TUMBLE_END(A.dateTime, INTERVAL '10' SECOND) AS endtime
        FROM
            auction A
        GROUP BY
            A.seller,
            TUMBLE(A.dateTime, INTERVAL '10' SECOND)
    ) A
             ON P.id = A.seller
                 AND P.starttime = A.starttime
                 AND P.endtime = A.endtime;