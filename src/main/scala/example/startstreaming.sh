 bin/spark-submit \
 --class example.StreamingExample \
 --master   spark://xiafan.local:7077 \
/Users/xiafan/git/xiafan/sparkcourse/target/riseandfall-1.0-SNAPSHOT-jar-with-dependencies.jar \
--input  file:///Users/xiafan/Documents/dataset/sparklecture/streaming \
--stopwords  file:///Users/xiafan/Documents/dataset/sparklecture/stopwords.txt \
--checkpoint  file:///Users/xiafan/Documents/dataset/sparklecture/checkpoint