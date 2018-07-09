package pingcap

import (
	"sync"
	"os"
	"math"
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
	handlers, parallelNum, divideNum, inMemoryDivideNum, err := readParallel(fileName)
	if err != nil {
		return nil, err
	}
	remainsDivideNum := divideNum
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
							if handlers[j].err != nil {
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
	remainsDivideNum -= inMemoryDivideNum

	// process divides which are not in memory
	for remainsDivideNum > 0 {
		for relativeIdx := 0; relativeIdx < inMemoryDivideNum; relativeIdx++ {
			wg.Add(1)
			divideIdx := divideNum - remainsDivideNum + relativeIdx
			go func() {
				defer wg.Done()
				m := make(map[element]byte)
				fileNames, tmpErr := getOutputFileNames(fileName, divideIdx)
				if tmpErr != nil {
					err = tmpErr
					return
				}
				for _, fileName := range fileNames {
					fr, tmpErr := os.Open(fileName)
					if tmpErr != nil {
						err = tmpErr
						return
					}
					tmpErr = readFile(fr, math.MaxInt64, func(e element) error {
						m[e] = 0
						return nil
					})
					if tmpErr != nil {
						err = tmpErr
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
		remainsDivideNum -= inMemoryDivideNum
	}

	return res, nil
}