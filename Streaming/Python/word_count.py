import os
import sys

os.environ['SPARK_HOME'] = "C:\spark"

sys.path.append("C:\spark")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    # sc = SparkContext('local')

    print("Imported")

except ImportError as e:
    print("Failed", e)

if __name__ == "__main__":
    from pyspark.streaming import StreamingContext
    sc = SparkContext("local[1]","Network  word count")
    ssc = StreamingContext(sc,1)

    lines = ssc.socketTextStream('localhost', 9999)
    words = lines.flatMap(lambda lines:lines.split(" "))

    pairs = words.map(lambda x:(x,1))

    wordcount = pairs.reduceByKey(lambda x,y:(x+y))

    wordcount.pprint()

    ssc.start()
    ssc.awaitTermination()
	
	
ssc.start()
ssc.awaitTermination()
