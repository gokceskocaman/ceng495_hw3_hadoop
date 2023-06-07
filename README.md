hadoop com.sun.tools.javac.Main *.java
jar cf Hw3.jar *.class
hadoop jar Hw3.jar total <modifiedInput.tsv> output_total