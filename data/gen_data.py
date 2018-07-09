#!/usr/bin/python

first = 1000
second = 10000

#first = 1
#second = 1

fw = open("test.csv", "w")
for i in range(first):
    for j in range (second):
        fw.write(str(i))
        fw.write("\t")
        fw.write(str(j))
        fw.write("\n")
        if j%2 == 0 or j%3 == 0:
            fw.write(str(i))
            fw.write("\t")
            fw.write(str(j))
            fw.write("\n")
fw.close()

