CREATE KEYSPACE holytwit WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};


drop MATERIALIZED VIEW holytwit.citycount;
drop materialized view holytwit.highestconnection;    
drop table holytwit.city_count;
drop table holytwit.htgraph;


CREATE TABLE holytwit.city_count (
    word text,
    place text,
    date text,
    count int,
    PRIMARY KEY ((word),place)
    ) WITH CLUSTERING ORDER BY (place desc);

CREATE TABLE holytwit.htgraph (
    word text,
    degree1 text,
    count int,
    PRIMARY KEY ((word),degree1)
    ) WITH CLUSTERING ORDER BY (degree1 desc);



CREATE MATERIALIZED VIEW holytwit.highestconnection AS
    SELECT degree1
    FROM holytwit.htgraph
    WHERE word IS NOT NULL AND count IS NOT NULL AND degree1 IS NOT NULL
    PRIMARY KEY ((word), count, degree1)
    WITH CLUSTERING ORDER BY (count desc);

CREATE MATERIALIZED VIEW holytwit.citycount AS
    SELECT place
    FROM holytwit.city_count
    WHERE word IS NOT NULL AND count IS NOT NULL AND place IS NOT NULL
    PRIMARY KEY ((word), count, place)
    WITH CLUSTERING ORDER BY (count desc);
