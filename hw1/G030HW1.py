"""MapReduce algorithms for counting the number of distinct triangles in an undirected graph

Implement two algorithms:
    1) MR_ApproxTCwithNodeColors: uses a hash function for partitioning
    2) MR_ApproxTCwithSparkPartitions: leverages the partitions provided by Spark

Usage:

    python GO30HW1.py C R <path/file.txt>
    
    Args:
        C: number of data partitions, integer
        R: number of executions of the method MR_ApproxTCwithNodeColors, integer
"""
from pyspark import SparkContext, SparkConf
from CountTriangles import CountTriangles

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

def main():
    #TODO
    print("main")

if __name__ == "__main__":
    main()