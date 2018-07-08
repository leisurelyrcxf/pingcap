package main

import (
	"fmt"
	"pingcap"
)

func main() {
	res, err := pingcap.GroupBy("test.csv", 6, 4)
	if err != nil {
		panic(err)
	}
	fmt.Printf("a\tavg(distinct b)\n")
	for _, row := range res.Rows {
		fmt.Printf("%v\t%v\n", row.A, row.Avg)
	}
}
