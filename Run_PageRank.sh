#!/bin/bash
# This script takes 0-3 arguments
# ./Run_PageRank.sh <Input folder> <Output folder> <Jar>
# If there is nothing specified default values are used

# Default values
links='/p3input/simplegraphformatted.txt'
titles='/p3input/simpletitles.txt'
reps='25'
jar='./target/CS435-PA3-1.0.jar'

# change links
if [ $# -gt 0 ]
  then
    links=$1
fi

# change titles
if [ $# -gt 1 ]
  then
    titles=$2
fi

# change reps
if [ $# -gt 2 ]
  then
    reps=$3
fi

if [ $# -gt 3 ]
  then
    jar=$4
fi


#nice -20 $SPARK_HOME/bin/spark-submit --class edu.colostate.cs.cs435.josiahm.pa3.drivers.JavaPageRank --master spark://madison.cs.colostate.edu:30138 --deploy-mode cluster ${jar} ${links} ${titles} ${reps}
nice -20 $SPARK_HOME/bin/spark-submit --class edu.colostate.cs.cs435.josiahm.pa3.drivers.JavaPageRank --master yarn --deploy-mode cluster --num-executors 16 --driver-memory 3g --executor-memory 3g --executor-cores 2 ${jar} ${links} ${titles} ${reps}
