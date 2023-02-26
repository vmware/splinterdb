# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
requestdistribution=zipfian
readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0
