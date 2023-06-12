from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict
import numpy as np
import statistics
import threading
import random
import sys


# After how many items should we stop?
THRESHOLD = 10000000
P = 8191

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, freq_dict, count_sketch_matrix, hash_function_parameters
    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size

    batch = batch.filter(lambda x: int(x)>=left and int(x)<= right)

    # Count exact frequency for each item
    batch_dict = batch.map(lambda x: (int(x), 1)).reduceByKey(lambda x, y: x + y).collectAsMap()
    for key in batch_dict:
        freq_dict[key] += batch_dict[key]
    
    # Count sketch update
    for x in batch.collect():
        for j in range(D):
            count_sketch_matrix[j, hash[j](int(x),W)] += g[j](int(x))
    
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))
    
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()



if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: D W left right K portExp"

    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()

    # INPUT READING
    D = sys.argv[1]
    assert D.isdigit() and int(D)>0, "D must be an integer greater than 0"
    D = int(D)

    W = sys.argv[2]
    assert W.isdigit() and int(W)>0, "W must be an integer greater than 0"
    W = int(W)

    left = sys.argv[3]
    assert left.isdigit() and int(left)>0, "left must be an integer greater than 0"
    left = int(left)

    right = sys.argv[4]
    assert right.isdigit() and int(right)>left, "right must be an integer greater than left"
    right = int(right)

    K = sys.argv[5]
    assert K.isdigit() and int(K)>0, "K must be an integer greater than 0"
    K = int(K)

    portExp = sys.argv[6]
    assert portExp.isdigit(), "portExp must be an integer"
    portExp = int(portExp)
    print("Receiving data from port =", portExp)
    
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    streamLength = [0]
    freq_dict = defaultdict(int)
    count_sketch_matrix = np.zeros([D,W], dtype=np.int32)
    hash = [ lambda x,C,a=a,b=b: ((a*x+b)%P)%C for a,b in [ (random.randint(1,P-1), random.randint(0,P-1)) for _ in range(D) ] ] # Python is strange...
    g = [ lambda x, a=a,b=b: (((a*x+b)%P)%2)*2-1 for a,b in [ (random.randint(1,P-1), random.randint(0,P-1)) for _ in range(D) ] ]

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    n_items = sum(freq_dict.values())
    n_distinct_items = len(freq_dict.keys())
    
    # 3. Exact F2
    F2_exact = sum((x**2)/(n_items**2) for x in freq_dict.values())
    
    # 4. Estimated F2
    F2_row_estimates = []
    for j in range(D):
        F2j = sum((count_sketch_matrix[j,k]**2)/(n_items**2) for k in range(W))
        F2_row_estimates.append(F2j)
    F2_estimate = (statistics.median(F2_row_estimates))
    
    # 5. Avg. error of the k-top frequencies
    frequencies = []
    error_sum = 0
    for key, fk_exact in sorted(freq_dict.items(), key=lambda item : item[1], reverse=True)[:K]:
        fk_estimate = statistics.median([ g[j](key)*count_sketch_matrix[j,hash[j](key,W) ] for j in range(D)])
        frequencies.append((key, fk_exact, fk_estimate))
        error_sum += abs(fk_exact-fk_estimate)/fk_exact
    error_avg = error_sum/K
    
    print(f"D = {D} W = {W}, [left,right] = [{left},{right}] K = {K} Port = {portExp}")
    print(f"Total number of items = {streamLength[0]}")
    print(f"Total number in items in [{left},{right}] = {n_items}")
    print(f"Number of distinct items in [{left},{right}] = {n_distinct_items}")
    if K <= 20:
        for i in range(K):
            print(f"Item {frequencies[i][0]} Freq = {frequencies[i][1]} Est. Freq = {frequencies[i][2]}")
    print(f"Avg error for top {K} = {error_avg}")
    print(f"F2 {F2_exact} F2 estimate {F2_estimate}")