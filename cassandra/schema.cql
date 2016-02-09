CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE holytwit.city_count (
    wordofinterest text,
    place text,
    count counter,
    PRIMARY KEY (wordofinterest, place)
    ) WITH CLUSTERING ORDER BY (place desc);

CREATE TABLE holytwit.city_count (
    wordofinterest text,
    place text,
    date text,
    count int,
    PRIMARY KEY ((wordofinterest,place,date),count)
    ) WITH CLUSTERING ORDER BY (count desc);

CREATE TABLE holytwit.htgraph (
    word text,
    degree1 text,
    place text,
    date text,
    count int,
    PRIMARY KEY ((word,degree1,place,date),count)
    ) WITH CLUSTERING ORDER BY (count desc);



CREATE MATERIALIZED VIEW holytwit.highest AS
    SELECT word, degree1, place, date, min
    FROM holytwit.degree1
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
