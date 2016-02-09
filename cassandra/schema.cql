CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE holytwit.city_count (
    word text,
    place text,
    date text,
    count int,
    PRIMARY KEY ((word),place,count)
    ) WITH CLUSTERING ORDER BY (place desc, count desc);

CREATE TABLE holytwit.htgraph (
    word text,
    degree1 text,
    count int,
    PRIMARY KEY ((word),degree1, count)
    ) WITH CLUSTERING ORDER BY (degree1 desc, count desc);



CREATE MATERIALIZED VIEW holytwit.highestconnection AS
    SELECT degree1
    FROM holytwit.htgraph
    WHERE word IS NOT NULL AND count IS NOT NULL AND degree1 IS NOT NULL
    PRIMARY KEY ((word), count, degree1)
    WITH CLUSTERING ORDER BY (count desc);

CREATE MATERIALIZED VIEW holytwit.citycount AS
    SELECT place
    FROM holytwit.city_coutn
    WHERE word IS NOT NULL AND count IS NOT NULL AND place IS NOT NULL
    PRIMARY KEY ((word), count, place)
    WITH CLUSTERING ORDER BY (count desc);
