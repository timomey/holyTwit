CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE holytwit.listofwords (
    word text,
    numberofwords int,
    time timestamp,
    PRIMARY KEY (word,time )
    );

CREATE TABLE holytwit.city_count (
    wordofinterest text,
    place text,
    count counter,
    PRIMARY KEY (wordofinterest, place)
    ) WITH CLUSTERING ORDER BY (place desc);

CREATE TABLE holytwit.city_count (
    wordofinterest text,
    place text,
    count int,
    PRIMARY KEY ((wordofinterest,place), count)
    ) WITH CLUSTERING ORDER BY (count desc);

CREATE TABLE holytwit.topicgraph (
    word text,
    connection text,
    count int,
    time timestamp,
    PRIMARY KEY (word, connection, time)
) WITH CLUSTERING ORDER BY (connection desc, time desc);



CREATE MATERIALIZED VIEW holytwit.name AS
    SELECT
    FROM
    WHERE
    PRIMARY KEY
    WITH CLUSTERING ORDER BY ();



    CREATE TABLE holytwit.degree1 (
        word text,
        degree1 text,
        place text,
        date text,
        min int,
        se60 int,
        se55 int,
        se50 int,
        se45 int,
        se40 int,
        se35 int,
        se30 int,
        se25 int,
        se20 int,
        se15 int,
        se10 int,
        se5 int,
        PRIMARY KEY ((word, degree1), place, date )
    )   WITH CLUSTERING ORDER BY (place desc, date desc);

CREATE TABLE holytwit.degree1_total (
    word text,
    degree1 text,
    date timestamp,
    min int,
    se60 int,
    se55 int,
    se50 int,
    se45 int,
    se40 int,
    se35 int,
    se30 int,
    se25 int,
    se20 int,
    se15 int,
    se10 int,
    se5 int,
    PRIMARY KEY ((word, degree1), min, date )
)   WITH CLUSTERING ORDER BY (min desc, date desc);
