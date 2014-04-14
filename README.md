MRsearchengine
==============

compile code:
============

ant

run code:
=========

Build InvertedList:


hadoop jar term-latest.jar -program invertedlist -input data/small_data.txt -output data/invertedlist -stopword data/stopword.txt


QuerySearch:

hadoop jar term-latest.jar -program querysearch -input data/invertedlist -output data/queryresult -args BM25args.txt -tmpdir tmp


Please download txt files from s3n://term-dataset/
