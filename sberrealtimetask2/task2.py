from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from ua_parser import user_agent_parser
from hdfs import Config
import subprocess
import hyperloglog
import time
import os

# Creating batches
client = Config().get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client', appName='Segments')

DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

BATCH_TIMEOUT = 2
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)

# Setting up variables
finished = False
printed = False

# Functions
# Function for flagging empty rdd's
def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True

# Function for printing the whole data
def print_at_the_end(rdd):
    global printed
    rdd.first()
    if finished and not printed:
        for row in rdd.sortBy(lambda x: x[1], ascending=False).take(3):
            print(f"{row[0]}\t{row[1]}")
        printed = True

# Function for formatting (segment, id)
def map_segment(x):
    segments = set()
    if "iPhone" in x[1]["device"]["family"]:
        segments.add(("seg_iphone", x[0]))
    if "Firefox" in x[1]["user_agent"]["family"]:
        segments.add(("seg_firefox", x[0]))
    if "Windows" in x[1]["os"]["family"]:
        segments.add(("seg_windows", x[0]))
    return segments

# Function for formatting (segment, id)
def aggregator(values, old):
    if old is None:
        old = hyperloglog.HyperLogLog(0.01)
    for v in values:
        old.add(v)
    return old

# Main logic
dstream.foreachRDD(set_ending_flag)

result = dstream.map(lambda line: line.strip().split("\t", 1)) \
                .map(lambda x: (x[0], user_agent_parser.Parse(x[1]))) \
                .flatMap(map_segment) \
                .updateStateByKey(aggregator) \
                .map(lambda x: (x[0], len(x[1]))) \
                .foreachRDD(print_at_the_end)

ssc.checkpoint("./checkpoint_instruments2025s14") 

# Start and stop by reaching condition
ssc.start()
while not printed:
     time.sleep(0.1)
ssc.stop()