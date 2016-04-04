from datetime import time
from datetime import datetime
from pyspark import SparkContext
from objects import *
import sys


# Calculates cell coordinates.
def cell(x, y):
    return (x - x % cell_size, y - y % cell_size)


# Splitting input line to device_id and alarm info
def prepare_alarms(line):
    device_id, timestamp, x, y = line.split(',')
    return (int(device_id), Alarm(int(x), int(y), is_night_time(int(timestamp))))


# Checks if alarm time was during the night - between 23:00 and 6:00.
def is_night_time(timestamp):
    hour = get_hour(timestamp)
    return hour < 6 or hour == 23


# Gets hour value from timestamp according to Moscow timezone.
def get_hour(timestamp):
    # should use platform's local timezone - in our case Moscow
    return datetime.fromtimestamp(timestamp).time().hour


# Creates accumulator of information about alarm counts from diffrent cells for particular device.
def create_combiner(alarm):
    my_map = {}
    my_map[cell(alarm.x, alarm.y)] = Cell(alarm)
    return my_map


# Adds information about new alarm to accumulator.
def merge_value(cells, alarm):
    if cell(alarm.x, alarm.y) not in cells:
        cells[cell(alarm.x, alarm.y)] = Cell(alarm)
    else:
        cells[cell(alarm.x, alarm.y)].add_info(alarm)
    return cells


# Merges data from two accumulators into one.
def merge_combiners(cells1, cells2):
    for key, value in cells1.iteritems():
        if key in cells2:
            cells2[key].add_cell(value)

    cells1.update(cells2)
    return cells1


# Picks a cell with highest number of night alarms.
# If there were less than five night alarms then night data might be incomplete, takes a cell with overall max count.
def get_home_cell(cells):
    result = max(cells, key=lambda cell: cell[3])
    if result[3] < 5:
        return max(cells, key=lambda cell: cell[2])
    else:
        return result


# Prepare the final output.
def format_output((device_id, cell)):
    return ','.join(map(str, [device_id, cell[0], cell[1]]))


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Usage: spark-submit --master local --py-files objects.py spark_job.py <input_hdfs_file> <output_hdfs_file>')
        sys.exit(1)

    # this value should be related to real distance size, can tweak it to optimize performance
    cell_size = 100

    sc = SparkContext("local", "Alert Button Job")
    rdd = sc.textFile(sys.argv[1])

    home_coordinates = (rdd.map(prepare_alarms)

                        # gathering all the alarm events inside the cells
                        .combineByKey(create_combiner, merge_value, merge_combiners)

                        # calculate average coordinates inside the cell
                        .mapValues(lambda cells: [v.avg_coords() for v in cells.values()])

                        # get the most higly populated cell
                        .mapValues(get_home_cell)

                        # prepare the final output
                        .map(format_output))

    home_coordinates.saveAsTextFile(sys.argv[2])
    