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

CREATE TABLE holytwit.topicgraph (
    word text,
    connection text,
    count int,
    time timestamp,
    PRIMARY KEY (word, connection, time)
) WITH CLUSTERING ORDER BY (connection desc, time desc);


CREATE TABLE holytwit.word_d1_place (
    word text,
    conn1 text,
    year int,
    month int,
    day int,
    hour int,
    min int,
    halfmin int,
    tensec int,
)


CREATE MATERIALIZED VIEW holytwit.name AS
    SELECT
    FROM
    WHERE
    PRIMARY KEY
    WITH CLUSTERING ORDER BY ()
