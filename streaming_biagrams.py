from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import string, time


translator = str.maketrans('', '', string.punctuation)

def normalize(line):
    
    return line.lower().translate(translator).split()

def make_bigrams(words):
    return [ (f"{words[i]} {words[i+1]}", 1)
             for i in range(len(words) - 1) ]

if __name__ == "__main__":
    sc = SparkContext(appName="BigramStream", master="local[2]")
    ssc = StreamingContext(sc, batchDuration=1)
    ssc.checkpoint("/tmp/spark_streaming_checkpoint")

    lines = ssc.socketTextStream("localhost", 9999)

    bigrams = lines.flatMap(lambda line: make_bigrams(normalize(line)))

    bigrams_window = bigrams.window(windowDuration = 3, slideDuration=2)

    bigram_counts = bigrams_window.reduceByKey(lambda a, b: a + b)

    bigram_counts.pprint()

    print("Listening on port 9999")
    
    ssc.start()
    ssc.awaitTermination()
