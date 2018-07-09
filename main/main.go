package main

import (
	"fmt"
	"pingcap"
	"sort"
	"math"
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
	res, err := pingcap.GroupBy("data/test.csv")
	if err != nil {
		panic(err)
	}
	fmt.Printf("a\tavg(distinct b)\n")
	sort.Sort(sortedRows(res.Rows))
	for _, row := range res.Rows {
		if math.Abs(row.Avg - 4999.5) > 1e-2 {
			fmt.Printf("%v\t%v\n", row.A, row.Avg)
		}
	}
}
