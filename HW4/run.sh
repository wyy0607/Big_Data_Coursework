echo "scan 'ywl3922', {COLUMNS => ['user', 'month', 'body'], FILTER => \"SingleColumnValueFilter('user', 'user', = ,'binary:allen-p')\"}" | hbase shell > hive_3.txt

echo "scan 'ywl3922', {COLUMNS => ['user', 'month', 'body'], FILTER => \"SingleColumnValueFilter('month', 'month', = ,'binary:Jan')\"}" | hbase shell > hive_4.txt

echo "scan 'ywl3922', {COLUMNS => ['user', 'month', 'body'], FILTER => \"SingleColumnValueFilter('user', 'user', = ,'binary:allen-p') AND SingleColumnValueFilter('month', 'month', = ,'binary:Jan')\"}" | hbase shell > hive_5.txt

echo "disable 'ywl3922'"

echo "drop 'ywl3922'"

exit