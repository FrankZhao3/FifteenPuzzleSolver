from pyspark import SparkContext
import Sliding, argparse

def solve_puzzle(master, output, height, width, slaves):
    global HEIGHT, WIDTH, level
    HEIGHT=height
    WIDTH=width
    level = 0

    sc = SparkContext(master, "python")

    """ YOUR CODE HERE """

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    countvalue = True # looping condition 
    curCount = 0 # number of elements in the RDD
    sol = Sliding.solution(WIDTH, HEIGHT) # solution's board configuration to the given WIDTH and HEIGHT
    a_dict = [(sol, level)]
    dataRDD = sc.parallelize(a_dict)

    # iteration stops when the number of elements in the RDD no longer change
    while countvalue:

        # using partition to reduce the budden for shuffling phase
        if level % 20 == 0:
            if level < 30:
                dataRDD = dataRDD.partitionBy(7, hash)
            else:
                dataRDD = dataRDD.partitionBy(12, hash)
        preCount = curCount
        dataRDD = dataRDD.flatMap(bfs_flat_map)
        dataRDD = dataRDD.reduceByKey(bfs_reduce)

        # count only the subset of RDD every other iteration (lazy evaluation)
        if level % 2 == 0:
            increasement = dataRDD.filter(lambda x: x[1] == level).count() # increasement is the number of newly generated children
            curCount += increasement
            if preCount == curCount:
                countvalue = False
        level += 1

    """ YOUR OUTPUT CODE HERE """
    # formating the output 
    output_string = ''
    temp = dataRDD.collect()
    # temp = sorted(temp, key=lambda x : x[1])  # sort the output
    for element in temp:
        output_string += str(element[1]) + ' ' + str(element[0]) + '\n'
    output(output_string)
    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    parser.add_argument("-S", "--slaves", type=int, default=6,
            help="number of slaves executing the job")
    args = parser.parse_args()

    global PARTITION_COUNT
    PARTITION_COUNT = args.slaves * 16

    # call the puzzle solver
    solve_puzzle(args.master, args.output, args.height, args.width, args.slaves)

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
