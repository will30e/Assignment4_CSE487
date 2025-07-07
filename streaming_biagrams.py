from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import string


translator = str.maketrans('', '', string.punctuation)

def normalize(line):
    
    return line.lower().translate(translator).split()

def make_bigrams(words):
    return [ (f"{words[i]} {words[i+1]}", 1)
             for i in range(len(words) - 1) ]

if __name__ == "__main__":
    # Step 1: Create SparkContext 
    sc = SparkContext(appName="BigramStream")
    ssc = StreamingContext(sc, batchDuration=1)
    ssc.checkpoint("/tmp/spark_streaming_checkpoint")