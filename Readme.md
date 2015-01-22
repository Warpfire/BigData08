Ratio's of Native and Forgeign Languages in twitter (using Map Reduce)
This code has been used to analyze a large dataset of twitter data for the Managing Big Data course of University Twente.
In our analysis we looked at the ratio's of native language and other languages in tweets.
We wanted to see if this changes on the basis of certain events.

Instalation
Our code is supported by maven which must be used to turn the source code into a jar file.

	1. Use Maven to install the code.

How to run the code
Once a jar has been constructed the code can be used to run several different map reduce jobs
The code only be run on a machine or cluster running hadoop.

All map reduce classes are located in nl.utwente.bigdata

To run the code using a command line use the following command:
	hadoop jar <Your jar file constructed using maven> nl.utwente.bigdata.<MapReduce Class> <inputLocation on hadoop file system> <outputLocation on hadoop file system> <remaining class specifc inputs>


The different MapReduce classes

MapReduce1:
	Starts a map reduce job which takes tweets and counts the combination of profile language and machine detected language.
MapReduce2:
	Mean to be used in support with MapReduce1 in combination with a hadoop fs -getmerge. 
	This MapReduce should get a merged version of the output of MapReduce1 as input.
	The output of MapReduce2 is a sorted version of this input file.
MapReduce2v1:
	It may be desirable to merge a few Profile languages (for example en and en_GB).
	This MapReduce can be used to replace the profile language with a fixed string which is given as additional input. 
MapReduce3:
	Starts a map reduce job which takes tweets and counts the combination of profile language, machine detected language and time with a granularity of hour.
MapReduce4:
	Mean to be used in support with MapReduce3 in combination with a hadoop fs -getmerge. 
	This MapReduce should get a merged version of the output of MapReduce3 as input.
	The output of MapReduce4 is a sorted version of this input file.
MapReduce4v1:
	It may be desirable to merge a few Profile languages (for example en and en_GB).
	This MapReduce can be used to replace the profile language with a fixed string which is given as additional input. 
MapReduce5:
	Meant to select a small subsets of data on the basis of time.
	The output is a count of combination profile language, machine detected language and time with a granularity of decaseconds.
	This MapReduce requires additional input in the following format <year> <Month> <Day> <hour> <minute> <duration> 
	where duration is how long the set should be after the given time in minutes.
