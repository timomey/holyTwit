CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE holytwit.city_count (
    wordofinterest text,
    place text,
    date text,
    count int,
    PRIMARY KEY ((wordofinterest,place),count,date)
    ) WITH CLUSTERING ORDER BY (count desc, date desc);

CREATE TABLE holytwit.htgraph (
    word text,
    degree1 text,
    date text,
    count int,
    PRIMARY KEY ((word,degree1),count,date)
    ) WITH CLUSTERING ORDER BY (count desc, date desc);



CREATE MATERIALIZED VIEW holytwit.highest AS
    SELECT word, degree1, place, date, min
    FROM holytwit.degree1
    WHERE
    PRIMARY KEY
    WITH CLUSTERING ORDER BY ();
