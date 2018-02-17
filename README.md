# MusicBox

## Step 1. Installing Spark
Source: https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b

## Step 2. Loading Raw Data with PySpark DataFrame
I have to say that spark dataframe is way less handy than pandas.dataframe for me. Anyway, to avoid some malformated lines, I have to use filter function on RDD first and then transform RDD to DataFrame

```python
def parseLine(line):
    fields = line.split('\t')
    if len(fields) == 10:
        uid = int(fields[0])
        device = str(fields[1])
        song_id = int(fields[2])
        song_type = int(fields[3])
        song_name = str(fields[4])
        singer = str(fields[5])
        play_time = int(fields[6])
        song_length = int(fields[7])
        paid_flag = int(fields[8])
        fn = str(fields[9])
        return Row(uid, device, song_id, song_type, song_name, singer, play_time, song_length, paid_flag, fn)
    else:
        return Row(None)
        
songs = lines.map(parseLine).filter(lambda x: len(x) == len(schema))
# Convert that to a DataFrame
songDataset = spark.createDataFrame(songs)
songDataset.show()
```
Here is the result:

+---------+---+--------+---+--------------------+--------------------+------+---+---+------------------+
|       _1| _2|      _3| _4|                  _5|                  _6|    _7| _8| _9|               _10|
+---------+---+--------+---+--------------------+--------------------+------+---+---+------------------+
|154422682|ar |20870993|  1|                 用情 |              狮子合唱团 | 22013|332|  0| 20170301_play.log|
|154421907|ip | 6560858|  0|             表情不要悲伤 |    伯贤&D.O.&张艺兴&朴灿烈 |    96|161|  0| 20170301_play.log|
|154422630|ar | 3385963|  1|Baby, Don't Cry(人...|                EXO |235868|235|  0| 20170301_play.log|
|154410267|ar | 6777172|  0|   3D-环绕音律1(3D Mix) |             McTaiM |   164|237|  0| 20170301_play.log|
|154407793|ar |19472465|  0|              刚好遇见你 |                曲肖冰 |    24|201|  0| 20170301_play.log|
|154422626|ar | 3198036|  1|              只唱给你听 |            SpeXial |275249|  0|  0| 20170301_play.log|
|154422681|ar |  891952|  0|   老男孩-(电影《老男孩》主题曲) |               筷子兄弟 |   300|300|  0| 20170301_play.log|
|154408091|ar | 4623962|  0|             预谋 许佳慧 |               网络歌手 |   243|243|  0| 20170301_play.log|
|154422571|ar |  703750|  0|            爸爸妈妈听我说 |               儿童歌曲 |   207|207|  0| 20170301_play.log|
|154417311|ar | 6491500|  0|        Stereo Love |        Edward Maya |    56|184|  0| 20170301_play.log|
|154421166|ar | 1967689|  0|    悟-(电影《新少林寺》主题曲) |                刘德华 |   139|275|  0| 20170301_play.log|
|154421859|ar | 6126586|  0|        老情歌(27秒铃声版) |                 童丽 |     4| 27|  0| 20170301_play.log|
|154422660|ar |11914644|  0|                 温柔 |杨丞琳[一闪一闪亮晶晶的钻石女士]...|   299|300|  0| 20170301_play.log|
|154422590|ar | 6468891|  0|                 演员 |                薛之谦 |   261|261|  0| 20170301_play.log|
|154419565|ar |15196649|  0|            我太帅了万人爱 |               MC马克 |     8| 65|  0| 20170301_play.log|
|154414286|ar | 7143177|  0|超越无限-(电影《听·见 林俊杰》...|                林俊杰 |    26|  0|  0| 20170301_play.log|
|154422089|ar |       0|  1|           预谋dj 许佳慧 |听30音乐网首发Qq369849635 |     7|345|  0| 20170301_play.log|
|154422443|ip | 6247282|  0|            假日(DJ版) |                罗百吉 |     1|277|  0| 20170301_play.log|
|154412729|ip | 4357368|  0|英文(Dj,2012 超好听的英文...|               慢摇舞曲 |     4|271|  0| 20170301_play.log|
|153985859|ar | 6762438|  0|         门铃声(1秒铃声版) |               手机铃声 |     1|  1|  0| 20170301_play.log|
+---------+---+--------+---+--------------------+--------------------+------+---+---+------------------+
