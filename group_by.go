package pingcap

import (
	"sync"
)

type Result struct {
	Rows []*ResultRow
}

type ResultRow struct {
	A   int64
	Avg float64
}

type aggStruct struct {
	sum float64
	count int64
}

func GroupBy(fileName string) (res *Result, err error) {
	handlers, parallelNum, _, inMemoryDivideNum, err := readParallel(fileName)
	if err != nil {
		return nil, err
	}
	res = &Result{
		Rows: make([]*ResultRow, 0, 100),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < inMemoryDivideNum; i++ {
		wg.Add(1)
		divideIdx := i
		go func() {
			defer wg.Done()
			m := make(map[element]byte)
			for {
				done := 0
				for j := 0; j < parallelNum; j++ {
					select {
					case element, ok := <- handlers[j].elements[divideIdx]:
						if !ok {
							if handlers[j].err != nil && err == nil {
								err = handlers[j].err
								break
							}
							done++
							continue
						}
						m[element] = 0
					default:
						continue
					}
					if err != nil {
						break
					}
				}
				if err != nil || done == parallelNum {
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

			qrResult := &Result{
				Rows: make([]*ResultRow, 0, 100),
			}
			for a := range mm {
				qrResult.Rows = append(qrResult.Rows, &ResultRow{A: a, Avg: float64(mm[a].sum)/float64(mm[a].count) })
			}
			mu.Lock()
			defer mu.Unlock()
			res.Rows = append(res.Rows, qrResult.Rows...)
		} ()
	}

	wg.Wait()
	if err != nil {
		return nil, err
	}
	return res, nil
}