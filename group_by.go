package pingcap

import (
	"sync"
	"fmt"
)

type result struct {
	rows []*resultRow
}

type resultRow struct {
	a int64
	avg float64
}

type aggStruct struct {
	sum float64
	count int64
}

func group_by(fileName string, parallelNum int, divideNum int) (res *result, err error) {
	handlers, err := readParallel(fileName, parallelNum, divideNum)
	if err != nil {
		return nil, err
	}
	res = &result{
		rows: make([]*resultRow, 0, 100),
	}
	var mu sync.Locker
	parallelNum = len(handlers)
	for i := 0; i < divideNum; i++ {
		go func() {
			m := make(map[element]byte)
			for {
				done := 0
				for j := 0; j < parallelNum; j++ {
					select {
					case element, ok := <- handlers[j].elements[i]:
						if !ok {
							done++
							continue
						}
						m[element] = 0
					default:
						continue
					}
				}
				if done == parallelNum {
					break
				}
			}

			mm := make(map[int64]aggStruct)
			for ele := range m {
				old := mm[ele.a]
				mm[ele.a] = aggStruct{
					sum:old.sum + float64(ele.b),
					count:old.count + 1,
				}
			}

			qrResult := &result{
				rows: make([]*resultRow, 0, 100),
			}
			for ele := range mm {
				qrResult.rows = append(qrResult.rows, &resultRow{a: ele, avg: float64(mm[ele].sum)/float64(mm[ele].count) })
			}
			mu.Lock()
			defer mu.Unlock()
			res.rows = append(res.rows, qrResult.rows...)
		} ()
	}

	return res, nil
}


func main() {
	res, err := group_by("test.csv", 4, 4)
	if err != nil {
		panic(err)
	}
	for _, row := range res.rows {
		fmt.Printf("a: %v, avg(distinct b): %v", row.a, row.avg)
	}
}