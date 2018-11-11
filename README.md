# CS435-PA3

### Requirements

1. Idealized PageRank
    * Find the top 10 Wikipedia articles by page rank calculated without tax
2. Taxation
    * Find the top 10 Wikipedia articles by page rank calculated with tax
3. Wikipedia Bomb
    * Simulate searching for article with `surfing` in the title and make the top article by page rank `Rocky_Mountain_National_Park`
    

**Due**: ~~November 6, 2018 Tuesday 5:00PM~~

**Extended**: November 10, 2018 Saturday 10:00PM


## Build Project

* Go to project root directory and run command
  * mvn package

## Run Project

* Go to project root directory and run command
  * ./Run_PageRank.sh \<Link file\> \<Titles File> \<Number of repetitions of page rank\>
  
The current script is set to run using the spark cluster. To run in yarn mode, go into the `./Run_PageRank.sh`
script and comment out the second to last line and uncomment the last line.

## Info
* Time for spark cluster run 15-20 minutes

## Issues
Yarn cluster mode keep running out of memory, I have changed the allotted memory from 2g, 3g, and 3.5g


# Comments 
The code given in the apache examples and the code snippets from recitation do not produce results that we
expected from lessons in class. We learnt that the sum of all the page ranks should be 1 but the given
code produces a sum that is close to the number of articles.

Because of this, I have sent two results sets and two main classes.

`ResultsFromGivenCode` contains the results from the code given

`ResultsExpectedFromClass` contains the results that the lesson taught us