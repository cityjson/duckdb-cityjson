```sql
select * from read_cityjson('https://storage.googleapis.com/cityjson/delft.city.jsonl');

create table cj as select * from read_cityjson('https://storage.googleapis.com/cityjson/delft.city.jsonl');
create table cjmeta as select * from cityjson_metadata('https://storage.googleapis.com/cityjson/delft.city.jsonl');
COPY (select * from cj) TO './test.city.json' (FORMAT cityjson, metadata_query 'select * from cjmeta');
```
