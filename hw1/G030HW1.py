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
import sys,os,time,random, statistics

P=8191
DEFAULT_KEY = 0

# utility function to measure the execution time of a function
def timeit(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()

        run_time = (time2-time1)*1000.0
        
        return (ret, run_time)
    return wrap

def color_vertices(edge, C, a, b):
    h_u = ((a * edge[0] + b) % P) % C
    h_v = ((a * edge[1] + b) % P) % C
    
    if h_u == h_v:
        return [(h_u, edge)]
    return []

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
    a = random.randint(0, P-1)
    b = random.randint(1, P-1)

    triangle_count = (RDD.flatMap(lambda x : color_vertices(x, C, a, b))                # <--- MAP PHASE (R1)
                        .groupByKey()                                                   # <--- SHUFFLE AND GROUPING (R1)
                        .flatMap(lambda x : [(DEFAULT_KEY, CountTriangles(x[1]))])      # <--- REDUCE PHASE (R1)
                        .reduceByKey(lambda x,y : (x + y)))                             # <--- REDUCE PHASE (R2)

    return (C**2)*triangle_count.take(1)[0][1]

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
    triangle_count = (RDD.mapPartitions(lambda x : [(DEFAULT_KEY,CountTriangles(x))])   # <--- MAP AND REDUCE (R1)
                        .reduceByKey(lambda x,y : (x+y)))                               # <--- REDUCE PHASE (R2)
    
    return (C**2)*triangle_count.take(1)[0][1]


def main():
    # checking number of cmd line parameters
    assert len(sys.argv) == 4, "Usage: python GO30HW1.py <C> <R> <path/file_name>"

	# Spark setup
    conf = SparkConf().setAppName('G030HW1')
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
    edges = rawData.map(lambda x: tuple(map(int, x.split(',')))).repartition(numPartitions=C).cache()
    numedges = edges.count()

    _, file_name = os.path.split(data_path)
    print(f"Dataset = {file_name}\nNumber of Edges = {numedges}\nNumber of Colors = {C}\nNumber of Repetitions = {R}")

    sum_time = 0
    runs_triangles = []
    for _ in range(0, R):
        triangles, time = MR_ApproxTCwithNodeColors(edges, C)
        runs_triangles.append(triangles)
        sum_time += time
    
    print(f"Approximation through node coloring\n"
          f"- Number of triangles (median over {R} runs) = {statistics.median(runs_triangles)}\n"
          f"- Running time (average over {R} runs) = {int(sum_time/R)} ms")

    triangles_partitions, time_partitions = MR_ApproxTCwithSparkPartitions(edges, C)

    print(f"Approximation through Spark partitions\n"
          f"- Number of triangles = {triangles_partitions}\n"
          f"- Running time = {int(time_partitions)} ms")

if __name__ == "__main__":
    main()