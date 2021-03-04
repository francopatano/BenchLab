
<img src="https://i.imgur.com/SJy1UpK.png" width="30%">

Benchmark your workload on different cluster specs and spark configs

Join up with cost data to tune cost/performance of your workload 




# BenchRunner #


* Read from the RunsConfig table
* Run all runs that do not have a runId via the REST API 
* Execute one at a time, wait 1 min between checks

* When job completes then
* Read from RunsHistorical table
* Query REST API Runs to see if run has completed
* Merge result data into RunsHistorical Table


# BenchDash #
* Show basic performance graph of runs 
* Enrich data with cloud costs 
* show workload price performance 
