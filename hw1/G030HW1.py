"""MapReduce algorithms for counting the number of distinct triangles in an undirected graph

Implement two algorithms:
    1) MR_ApproxTCwithNodeColors: uses a hash function for partitioning
    2) MR_ApproxTCwithSparkPartitions: leverages the partitions provided by Spark

Usage:

    python GO30HW1.py <C> <R> <path/file.txt>
    
    Args:
        C: number of data partitions, integer
        R: number of executions of the method MR_ApproxTCwithNodeColors, integer
"""
from pyspark import SparkContext, SparkConf
from CountTriangles import CountTriangles
import re,sys,os,time,random

P=8191

def color_vertices(edge, C, a, b):
    h_u = ((a * edge[0] + b) % P) % C
    h_v = ((a * edge[1] + b) % P) % C
    
    if h_u == h_v:
        return (h_u, edge)
    return (-1, edge)

#@timeit
def MR_ApproxTCwithNodeColors(RDD, C):
    """First algorithm for Triangle Counting

        Define a hash function:
            h(u)=((a*u+b)mod p)mod C
        with p=8191, a random integer in [0,p-1] and b random integer in [1,p-1]
        Two Rounds:
            - Round 1: create C subsets, the i-th subset consists of all edges (u,v) such that h(u)=h(v)=i. Compute the number of triangles formed by the edges of each subsets separately
            - Round 2: compute and return the final estimate of the number of triangles

        Args:
            RDD: RDD (Resilient Distributed Dataset) of edges
            C: number of colors

        Returns:
            number of triangles formed by the input edges

        Raises:
    """
    a = random.randint(0, P-1)
    b = random.randint(1, P-1)

    triangle_count = (RDD.map(lambda x : color_vertices(x, C, a, b)) # R1 MAP PHASE
                        .groupByKey()
                        .filter(lambda x : x[0] != -1) # R1 SHUFFLE + GROUPING
                        .flatMap(lambda x : [(0, CountTriangles(list(x[1])))]) # R1 REDUCE PHASE
                        .reduceByKey(lambda x,y : x+y)) #R2 REDUCE PHASE

    return C**2*triangle_count.collect()[0][1]

#@timeit
def MR_ApproxTCwithSparkPartitions(RDD, C):
    """Second Algorithm for Triangle Counting

        Uses Spark's partitions.
        Two Rounds:
            - Round 1: partition randomly the edges into C subset. Compute the number of triangles formed by the edges of each subsets separately
            - Round 2: compute and return the final estimate of the number of triangles
        
        Args:
            RDD: RDD (Resilient Distributed Dataset) of edges
            C: number of colors

        Returns:
            number of triangles formed by the input edges

        Raises:
    """
    #TODO
    return

# utility function to measure the execution time of a function
# TODO: for the first function they want the average execution time, rn we are outputting each execution
def timeit(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        print('{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0))

        return ret
    return wrap

def main():
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(sys.argv) == 4, "Usage: python GO30HW1.py <C> <R> <path/file_name>"

	# SPARK SETUP
    conf = SparkConf().setAppName('TriangleCounting')
    sc = SparkContext(conf=conf)

    # parse C parameter
    C = sys.argv[1]
    assert C.isdigit(), "C must be an integer"
    C = int(C)
    assert C>0, "C must be at least 1"

    # parse R parameter
    R = sys.argv[2]
    assert R.isdigit(), "R must be an integer"
    R = int(R)
    assert R>0, "R must be at least 1"

    # parse data_path
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"

    rawData = sc.textFile(data_path, minPartitions=C)
    edges = rawData.map(lambda x: tuple(map(int, x.split(',')))) # convert the string edges into tuple of int
    edges.repartition(numPartitions=C).cache()
    numedges = edges.count()

    _, file_name = os.path.split(data_path)
    print(f"Dataset = {file_name}\nNumber of Edges = {numedges}\nNumber of Colors = {C}\nNumber of Repetitions = {R}")

    sum_triangles = 0
    sum_time = 0
    for i in range(0, R):
        start_time = time.time()
        sum_triangles += MR_ApproxTCwithNodeColors(edges, C)
        sum_time += (time.time() - start_time)*1000
    print(f"Approximation through node coloring\n- Number of triangles (median over {R} runs) = {int(sum_triangles/R)}\n - Running time (average over {R} runs) = {int(sum_time/R)}")

if __name__ == "__main__":
    main()