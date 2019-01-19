# maprdb-json-datatype-checker

Utility program to identify data types of field values for MapR DB Json table.

## Getting Started

Clone the repository and build it using maven.

```
git clone git@github.com:kr-arjun/maprdb-json-datatype-checker.git
cd maprdb-json-datatype-checker/
mvn clean install
```

### Usage


```
hadoop jar maprdb-json-typecheck-0.0.1-SNAPSHOT.jar com.mapr.support.MaprDBJsonTypeFinder <json table> 
<output directory> [optional comma seperated field names]
```

### Sample Usage:

Below is sample table with fields ts and ts2.

```
$mapr dbshell
MapR-DB Shell
maprdb mapr:> find /tmp/json_tbl_test
{"_id":"01","ts":{"$date":"1995-03-08T05:21:56.000Z"}}
{"_id":"06","ts":{"$date":"1995-03-08T05:21:56.000Z"}}
{"_id":"2","ts":1}
{"_id":"3","ts":"3","ts1":1}
{"_id":"4","ts":"3","ts1":1}
5 document(s) found.
maprdb mapr:>
```

#### To find all field data types:
```
hadoop jar maprdb-json-typecheck-0.0.1-SNAPSHOT.jar com.mapr.support.MaprDBJsonTypeFinder /tmp/json_tbl_test /tmp/json_tbl_type_output

Output:

$ hadoop fs -cat /tmp/json_tbl_type_output/*
columnName=_id|java.lang.String=5|
columnName=ts|java.lang.Double=1|java.lang.String=2|org.ojai.types.OTimestamp=2|
columnName=ts1|java.lang.Double=2|
[mapr@mapr6-78 ~]$

```

#### To find data types for subset of fields.

```
hadoop jar maprdb-json-typecheck-0.0.1-SNAPSHOT.jar com.mapr.support.MaprDBJsonTypeFinder /tmp/json_tbl_test /tmp/json_tbl_type_subset_output "_id,ts"

Output: 

$ hadoop fs -cat /tmp/json_tbl_type_subset_output/*
columnName=_id|java.lang.String=5|
columnName=ts|java.lang.Double=1|java.lang.String=2|org.ojai.types.OTimestamp=2|
$
```


