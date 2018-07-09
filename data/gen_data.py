#!/usr/bin/python

first = 1000000
second = 10000

#first = 1
#second = 1

fw = open("test.csv", "w")
for i in range(first):
	fw.write("1")
	fw.write("\t")
	fw.write("1")
	fw.write("\n")

for i in range(second):
	fw.write("1")
	fw.write("\t")
	fw.write("2")
	fw.write("\n")


for i in range(first):
	fw.write("1234")
	fw.write("\t")
	fw.write("1")
	fw.write("\n")

for i in range(second):
	fw.write("1234")
	fw.write("\t")
	fw.write("2")
	fw.write("\n")


for i in range(first):
	fw.write("3")
	fw.write("\t")
	fw.write("1")
	fw.write("\n")

for i in range(second):
	fw.write("3")
	fw.write("\t")
	fw.write("2")
	fw.write("\n")


for i in range(11111):
	fw.write("3")
	fw.write("\t")
	fw.write("3")
	fw.write("\n")

for i in range(11):
	fw.write("3")
	fw.write("\t")
	fw.write("4")
	fw.write("\n")


for i in range(first):
	fw.write("0")
	fw.write("\t")
	fw.write("0")
	fw.write("\n")

for i in range(second):
	fw.write("0")
	fw.write("\t")
	fw.write("0")
	fw.write("\n")


for i in range(first):
	fw.write("2")
	fw.write("\t")
	fw.write("2")
	fw.write("\n")

for i in range(second):
	fw.write("2")
	fw.write("\t")
	fw.write("2")
	fw.write("\n")

fw.close()

