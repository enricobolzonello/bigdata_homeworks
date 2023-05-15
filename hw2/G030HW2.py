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
from collections import defaultdict

P=8191
DEFAULT_KEY = 0

def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

# utility function to measure the execution time of a function
def timeit(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()

        run_time = (time2-time1)*1000.0
        
        return (ret, run_time)
    return wrap

def hash_function(e, C, a, b):
    return ((a * e +b) % P) % C

def color_vertices(edge, C, a, b):
    h_u = hash_function(edge[0], C, a, b)
    h_v = hash_function(edge[1], C, a, b)
    
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

def createpairs(edge, C, a, b):
    h_u = hash_function(edge[0], C, a, b)
    h_v = hash_function(edge[1], C, a, b)
    pairs = []
    for i in range(0, C):
        '''s = ""
        for l in sorted((h_u,h_v,i)):
            s = s + l
        pairs.append((l, edge))'''
        pairs.append((tuple(sorted((h_u,h_v,i))), edge))
        
    return pairs
            

@timeit
def MR_ExactTC(RDD, C):
    a = random.randint(0, P-1)
    b = random.randint(1, P-1)

    triangle_count = (RDD.flatMap(lambda x : createpairs(x, C, a, b))
                        .groupByKey()
                        .flatMap(lambda x : [(DEFAULT_KEY, countTriangles2(x[0], x[1], a, b, P, C))])
                        .reduceByKey(lambda x,y : x+y)
                    )
    return triangle_count.take(1)[0][1]

def main():
    # checking number of cmd line parameters
    assert len(sys.argv) == 5, "Usage: python GO30HW1.py <C> <R> <F> <path/file_name>"

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

    # parse F parameter
    F = sys.argv[3]
    assert F.isdigit(), "F must be a binary value"
    F = int(F)
    assert F == 0 or F == 1, "F must be 0 or 1"

    # parse data_path
    data_path = sys.argv[4]
    assert os.path.isfile(data_path), "File or folder not found"

    rawData = sc.textFile(data_path, minPartitions=16)
    edges = rawData.map(lambda x: tuple(map(int, x.split(',')))).repartition(numPartitions=16).cache()
    numedges = edges.count()

    _, file_name = os.path.split(data_path)
    print(f"Dataset = {file_name}\nNumber of Edges = {numedges}\nNumber of Colors = {C}\nNumber of Repetitions = {R}")

    sum_time = 0
    runs_triangles = []
    if F == 0:
        for _ in range(0, R):
            triangles, time = MR_ApproxTCwithNodeColors(edges, C)
            runs_triangles.append(triangles)
            sum_time += time
    
        print(f"Approximation through node coloring\n"
            f"- Number of triangles (median over {R} runs) = {statistics.median(runs_triangles)}\n"
            f"- Running time (average over {R} runs) = {int(sum_time/R)} ms")
    elif F == 1:
        for _ in range(0,R):
            triangles, time = MR_ExactTC(edges, C)
            sum_time += time
        
        print(f"Exact algorithm with node coloring\n"
            f"- Number of triangles = {triangles}\n"
            f"- Running time (average over {R} runs) = {int(sum_time/R)} ms")


if __name__ == "__main__":
    main()