CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE holytwit.city_count (
    word text,
    place text,
    count int,
    PRIMARY KEY ((word),count,place)
    ) WITH CLUSTERING ORDER BY ( count desc,place desc);

CREATE TABLE holytwit.htgraph (
    word text,
    degree1 text,
    count int,
    PRIMARY KEY ((word),count,degree1)
    ) WITH CLUSTERING ORDER BY (count desc, degree1 desc);



CREATE MATERIALIZED VIEW holytwit.highest AS
    SELECT word, degree1, place, date, min
    FROM holytwit.degree1
    WHERE
    PRIMARY KEY
    WITH CLUSTERING ORDER BY ();
