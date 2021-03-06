package main

import (
	"fmt"
	"pingcap"
	"sort"
	"math"
	"time"
)

type sortedRows []*pingcap.ResultRow

// Len is the number of elements in the collection.
func (sr sortedRows) Len() int {
	return len(sr)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (sr sortedRows) Less(i, j int) bool {
	return sr[i].A < sr[j].A
}

// Swap swaps the elements with indexes i and j.
func (sr sortedRows) Swap(i, j int) {
	sr[i], sr[j] = sr[j], sr[i]
}

func main() {
	startTime := time.Now()
	res, err := pingcap.GroupBy("data/test.csv")
	if err != nil {
		panic(err)
	}
	duration := time.Now().Sub(startTime)
	fmt.Printf("a\tavg(distinct b)\n")
	sort.Sort(sortedRows(res.Rows))

	errNum := 0
	for rowIdx, row := range res.Rows {
		if row.A != int64(rowIdx) || math.Abs(row.Avg - 4999.5) > 1e-2 {
			fmt.Printf("%v\t%v\n", row.A, row.Avg)
			errNum++
		}
	}
	if errNum == 0 && len(res.Rows) == 1000 {
		fmt.Println("test succeeded")
		fmt.Println("cost %v", duration)
	} else {
		fmt.Println("regressioned")
	}
}
