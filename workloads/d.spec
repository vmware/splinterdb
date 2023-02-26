# Yahoo! Cloud System Benchmark
# Workload D: Read latest workload
#   Application example: user status updates; people want to read the latest
#                        
#   Read/update/insert ratio: 95/0/5
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: latest

# The insert order for this is hashed, not ordered. The "latest" items may be 
# scattered around the keyspace if they are keyed by userid.timestamp. A workload
# which orders items purely by time, and demands the latest, is very different than 
# workload here (which we believe is more typical of how people build systems.)

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.95
updateproportion=0
scanproportion=0
insertproportion=0.05

requestdistribution=latest

