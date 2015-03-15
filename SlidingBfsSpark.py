from pyspark import SparkContext
import Sliding, argparse

def bfs_map(value):
    return (value[1], value[0])

def bfs_reduce(value1, value2):
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
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)
    end = sol[::-1]

    def creating_sc_data(puzzle, level, already_seen):
        sc_data = []
        if puzzle not in already_seen or already_seen[puzzle] > level:
            already_seen[puzzle] = level
            children = Sliding.children(WIDTH, HEIGHT, puzzle)
            for child in children:
                next_levels = creating_sc_data(child, level + 1, already_seen)
                for data in next_levels:
                    sc_data.append(data)
            sc_data.append((puzzle, level))
        return sc_data

    data = sc.parallelize(creating_sc_data(sol, level, {}))

    print_this_shit = data.reduceByKey(bfs_reduce) \
                        .sortByKey() \
                        .map(bfs_map) \
                        .sortByKey() \
                        .collect()

    for shit in print_this_shit:
        output(str(shit[0]) + " " + str(shit[1]))

    sc.stop()

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
