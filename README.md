to convert cvs to tvs:

$ python3 csvToTsv.py <input.csv> 

this will create 'modifiedInput.tsv' file.

compilation: 

hadoop com.sun.tools.javac.Main *.java 

jar cf Hw3.jar *.class

running:

hadoop jar Hw3.jar total modifiedInput.tsv output_total 

hadoop jar Hw3.jar average modifiedInput.tsv output_average

hadoop jar Hw3.jar employment modifiedInput.tsv output_employment 

hadoop jar Hw3.jar ratescore modifiedInput.tsv output_ratescore 

hadoop jar Hw3.jar genrescore modifiedInput.tsv output_genrescore
