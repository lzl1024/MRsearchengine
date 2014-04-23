MRsearchengine
==============

compile code:
============

ant

run code on aws:
=========

Jar: s3n://term-dataset/term-latest.jar

Build InvertedList:


-program invertedlist -input s3n://term-dataset/long_abstracts_en.nt -output s3n://term-dataset/invertedlist -stopword s3n://term-dataset/stopword.txt


Query Expansion:

-program queryexpansion -input s3n://term-dataset/invertedlist1/ -output s3n://term-dataset/queryexpansion -tmpdir tmp


QuerySearch:

h-program querysearch -input s3n://term-dataset/invertedlist1/ -output s3n://term-dataset/queryresult -args s3n://term-dataset/BM25args.txt -tmpdir tmp -expIn s3n://term-dataset/queryexpansion


Please download txt files from s3n://term-dataset/
