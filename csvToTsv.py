import sys, csv

csv.writer(open("modifiedInput.tsv", 'w+'), delimiter='\t').writerows(csv.reader(open(sys.argv[1])))