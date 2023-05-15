from pyspark import SparkContext, SparkConf
import sys,os,time,random, statistics
from collections import defaultdict

P=8191
DEFAULT_KEY = 0

def CountTriangles(edges):
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    triangle_count = 0

    for u in neighbors:
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    return triangle_count


def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    colors = list(colors_tuple)  
    neighbors = defaultdict(set)
    node_colors = dict()
    for edge in edges:
        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    triangle_count = 0

    for v in neighbors:
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    if w > u and w in neighbors[v]:
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        if colors==triangle_colors:
                            triangle_count += 1
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

# hash function used by the two algorithms
def hash_function(e, C, a, b):
    return ((a * e +b) % P) % C

# map function used by MR_ApproxTCwithNodeColors
def color_vertices(edge, C, a, b):
    h_u = hash_function(edge[0], C, a, b)
    h_v = hash_function(edge[1], C, a, b)
    
    if h_u == h_v:
        return [(h_u, edge)]
    return []

# map function used by MR_ExactTC
def create_pairs(edge, C, a, b):
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
def MR_ApproxTCwithNodeColors(RDD, C):
    a = random.randint(0, P-1)
    b = random.randint(1, P-1)

    triangle_count = (RDD.flatMap(lambda x : color_vertices(x, C, a, b))                # <--- MAP PHASE (R1)
                        .groupByKey()                                                   # <--- SHUFFLE AND GROUPING (R1)
                        .flatMap(lambda x : [(DEFAULT_KEY, CountTriangles(x[1]))])      # <--- REDUCE PHASE (R1)
                        .reduceByKey(lambda x,y : (x + y)))                             # <--- REDUCE PHASE (R2)

    return (C**2)*triangle_count.take(1)[0][1]

@timeit
def MR_ExactTC(RDD, C):
    a = random.randint(0, P-1)
    b = random.randint(1, P-1)

    triangle_count = (RDD.flatMap(lambda x : create_pairs(x, C, a, b))                                    # <--- MAP PHASE (R1)        
                        .groupByKey()                                                                     # <--- SHUFFLE AND GROUPING (R1)
                        .flatMap(lambda x : [(DEFAULT_KEY, countTriangles2(x[0], x[1], a, b, P, C))])     # <--- REDUCE PHASE (R1)
                        .reduceByKey(lambda x,y : x+y))                                                   # <--- REDUCE PHASE (R2)
    
    return triangle_count.take(1)[0][1]

def main():
    # checking number of cmd line parameters
    if len(sys.argv) != 5:
        print("Usage: python GO30HW1.py <C> <R> <F> <path/file_name>")
        return

	# Spark setup
    conf = SparkConf().setAppName('G030HW1')
    sc = SparkContext(conf=conf)

    # parse C parameter
    C = sys.argv[1]
    if (not C.isdigit()) or (int(C) < 1):
        print("C must be an integer grater than 1")
        return
    C = int(C)
    
    # parse R parameter
    R = sys.argv[2]
    if (not R.isdigit()) or (int(R) < 1):
        print("R must be an integer grater than 1")
        return
    R = int(R)

    # parse F parameter
    F = sys.argv[3]
    if (not F.isdigit()) or (not (int(F) == 0 or int(F) == 1)):
        print("F must be 0 or 1")
        return
    F = int(F)

    # parse data_path
    data_path = sys.argv[4]
    if not os.path.isfile(data_path):
        print("File or folder not found")
        return

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