from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import numpy as np
import random
import statistics

# After how many items should we stop?
THRESHOLD = 10000000
P = 8191

def hash_function(t,x,C):
    return ((t[0]*x + t[1]) % P) % C


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

    # 2. Count frequency for each item
    batch_dict = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: i1 + i2).collectAsMap()
    for key in batch_dict:
        freq_dict[key] = freq_dict.get(key, 0) + batch_dict[key]
    
    # COUNT SKETCH
    for x in batch.collect():
        for j in range(0, D):
            count_sketch_matrix[j,hash_function(hash_function_parameters[j] ,int(x), W)] += (hash_function(hash_function_parameters[j], int(x), 2)*2 - 1) # update
            
    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: D W left right K portExp"

    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    
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
    W = int(D)

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
    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    freq_dict = {} # Dictionary for exact frequency
    count_sketch_matrix = np.empty([D,W]) #matrix for the count sketch
    hash_function_parameters = [(random.randint(1,P-1), random.randint(1,P-1)) for _ in range(D)]

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # COMPUTE AND PRINT FINAL STATISTICS
    n_items = sum(freq_dict.values())
    n_distinct_items = len(freq_dict.keys())
    # 3. Exact F2
    F2 = sum((x**2)/n_items**2 for x in freq_dict.values())
    # 4. Estimated F2
    F2_estimate = 0
    for key in freq_dict:
        fu_list = []
        for j in range(0,D):
            fu_list.append((hash_function(hash_function_parameters[j], key, 2)*2 - 1) * count_sketch_matrix[j,hash_function(hash_function_parameters[j] ,key, W)])
        F2_estimate += statistics.median(fu_list)**2/n_items**2

    print("Total number of items = ", streamLength[0])
    print(f"Total number in items in [{left},{right}] = ", n_items)
    print(f"Number of distinct items in [{left},{right}] = ", n_distinct_items)

    if K <= 20:
        for i, pair in enumerate(dict(sorted(freq_dict.items(), key=lambda item: item[1], reverse=True))):
            print(f"Item {pair} Freq = {freq_dict[pair]} Est. Freq = {F2_estimate}")

            if i == K:
                break
    
    print(f"F2 {F2} F2 estimate temp")