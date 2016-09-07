from pyspark import SparkContext
import Sliding, argparse

def bfs_flat_map(value):
    """ YOUR CODE HERE """
    lst = []
    lst.append(value)
    if value[1] is level - 1:
        for element in Sliding.children(WIDTH, HEIGHT, value[0]):
            lst.append((element, level))
    return lst
def bfs_reduce(value1, value2):
    """ YOUR CODE HERE """
    return min(value1, value2)

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT = height
    WIDTH = width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job
    result = []
    outputResult = ''
    counter = 2
    counter2 = 0
    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    # sc.parallelized()
    # counter = 0;
    sol = Sliding.solution(width, height)
    Rdd = sc.parallelize([(sol, level)])
    while counter != counter2:
        #if level % 2 == 0:
        counter = counter2
        if level % 10 == 0:
            Rdd = Rdd.partitionBy(7, hash)
        level += 1
        Rdd = Rdd.flatMap(bfs_flat_map) \
                 .reduceByKey(bfs_reduce)
        #if level % 2 == 0:
        counter2 = Rdd.count()
    """ YOUR OUTPUT CODE HERE """
    # formating the output 
    for element in Rdd.collect():
        result.append((element[1], element[0]))
    result = sorted(result)
    for element in result:
         outputResult += str(element[0]) + ' ' + str(element[1]) + '\n'
    output(outputResult)


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
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()