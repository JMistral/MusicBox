# MusicBox

## Step 1. Installing Spark
Source: https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b

## Step 2. Loading Raw Data with PySpark DataFrame
I have to say that spark dataframe is way less handy than pandas.dataframe for me. Anyway, to avoid some malformated lines, I have to use filter function on RDD first and then transform RDD to DataFrame

```python
from pyspark.sql.types import *

def parseLine(line):
    fields = line.split('\t')
    if len(fields) == 10:
        try:
            uid = float(fields[0])
            device = str(fields[1])
            song_id = str(fields[2])
            song_type = float(fields[3])
            song_name = str(fields[4])
            singer = str(fields[5])
            play_time = str(fields[6])
            song_length = float(fields[7])
            paid_flag = float(fields[8])
            fn = str(fields[9])
            return Row(uid, device, song_id, song_type, song_name, singer, play_time, song_length, paid_flag, fn)
        except:
            return Row(None)
    else:
        return Row(None)


schema = StructType([StructField('uid', FloatType(), False),
                     StructField('device', StringType(), True),
                     StructField('song_id', StringType(), False),
                     StructField('song_type', FloatType(), True),
                     StructField('song_name', StringType(), True),
                     StructField('singer', StringType(), True),
                     StructField('play_time', StringType(), False),
                     StructField('song_length', FloatType(), True),
                     StructField('paid_flag', FloatType(), True),
                     StructField('fn', StringType(), True),])
```

```python
songs = lines.map(parseLine).filter(lambda x: len(x) == len(schema))
# Convert that to a DataFrame
songDataset = spark.createDataFrame(songs,schema).cache()
songDataset.show()
```
Here is the result:

+------------+------+---------+---------+--------------------+--------------------+---------+-----------+---------+------------------+
|         uid|device|  song_id|song_type|           song_name|              singer|play_time|song_length|paid_flag|                fn|
+------------+------+---------+---------+--------------------+--------------------+---------+-----------+---------+------------------+
|1.54422688E8|   ar |20870993 |      1.0|                 用情 |              狮子合唱团 |   22013 |      332.0|      0.0| 20170301_play.log|
|1.54421904E8|   ip | 6560858 |      0.0|             表情不要悲伤 |    伯贤&D.O.&张艺兴&朴灿烈 |      96 |      161.0|      0.0| 20170301_play.log|
|1.54422624E8|   ar | 3385963 |      1.0|Baby, Don't Cry(人...|                EXO |  235868 |      235.0|      0.0| 20170301_play.log|
|1.54410272E8|   ar | 6777172 |      0.0|   3D-环绕音律1(3D Mix) |             McTaiM |     164 |      237.0|      0.0| 20170301_play.log|
|1.54407792E8|   ar |19472465 |      0.0|              刚好遇见你 |                曲肖冰 |      24 |      201.0|      0.0| 20170301_play.log|
|1.54422624E8|   ar | 3198036 |      1.0|              只唱给你听 |            SpeXial |  275249 |        0.0|      0.0| 20170301_play.log|
|1.54422688E8|   ar |  891952 |      0.0|   老男孩-(电影《老男孩》主题曲) |               筷子兄弟 |     300 |      300.0|      0.0| 20170301_play.log|
+------------+------+---------+---------+--------------------+--------------------+---------+-----------+---------+------------------+

### Check some statistics of the data:

```python
songDataset.describe().show()
```
+-------+--------------------+---------+--------------------+-------------------+---------+--------------------+-------------------+------------------+---------+--------------------+
|summary|                 uid|   device|             song_id|          song_type|song_name|              singer|          play_time|       song_length|paid_flag|                  fn|
+-------+--------------------+---------+--------------------+-------------------+---------+--------------------+-------------------+------------------+---------+--------------------+
|  count|           164264529|164264529|           164264529|          164264529|164264529|           164264529|          164264529|         164264529|164264529|           164264529|
|   mean|1.3238275802376163E8|     null|1.233773654943951...|0.14990355586749954| Infinity|2.069222247928341...| 204343.58310764717|-66.93764578220485|      0.0|                null|
| stddev|6.4977108791913636E7|     null|3.724137957677398...| 0.3858627542831314|      NaN|1.842783835874818...|5.244159580504907E8|1066716.9038908319|      0.0|                null|
|    min|                 0.0|         |                    |                0.0|         |                    |                   |     -2.14748365E9|      0.0|   20170301_play.log|
|    max|         1.6926232E8|       wp|             9999985|                3.0|       🙄|          😝😝😝😝😝|               nan |      1.34396621E9|      0.0| 20170512_3_play.log|
+-------+--------------------+---------+--------------------+-------------------+---------+--------------------+-------------------+------------------+---------+--------------------+

```python
songDataset.groupBy('uid').count().orderBy('count', ascending = False).show(truncate=False)
```
+------------+-------+
|uid         |count  |
+------------+-------+
|1685126.0   |8124398|
|3.7025504E7 |5903930|
|751824.0    |4554232|
|1791497.0   |3376118|
|497685.0    |3031361|
|1062806.0   |2354592|
|736305.0    |1848836|
|0.0         |1201159|
|1749320.0   |835164 |
|4.6532272E7 |500025 |
|1679121.0   |488577 |
|2.8638488E7 |469655 |
|637650.0    |243074 |
|1.5594824E8 |217992 |
|533817.0    |173401 |
|3.2166204E7 |156643 |
|6.4268008E7 |150171 |
|2.6036032E7 |114145 |
|3.2104144E7 |99175  |
|1.67982848E8|82687  |
+------------+-------+
only showing top 20 rows

