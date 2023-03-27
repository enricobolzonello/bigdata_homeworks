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
import re,sys,os
import time

P=8191

@timeit
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
    #TODO
    return

@timeit
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

# helper function to remove the directory from the data path
def rem_dir(s):
    find_backslash = s.find("/")

    if find_backslash != -1:
        s = s[find_backslash+1:]
    return s

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

    # parse R parameter
    R = sys.argv[2]
    assert R.isdigit(), "R must be an integer"
    R = int(R)

    # parse data_path
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"

    rawData = sc.textFile(data_path, minPartitions=C)
    edges = rawData.map(lambda x: tuple(map(int, x.split(',')))) # convert the string edges into tuple of int
    edges.repartition(numPartitions=C).cache()

    print(f"File name: {rem_dir(data_path)}\nNumber of partitions C: {C}\nNumber of executions of the second algorithm R: {R}\nNumber of edges: {edges.count()}")

if __name__ == "__main__":
    main()