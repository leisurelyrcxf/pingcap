package pingcap

import (
	"os"
	"math"
	"io"
	"fmt"
	"strconv"
)

const fileInputBufferSize = 1024
const parallelMinSize = 1024*1024
const csvDelimiter = '\t'

type element struct {
	a int64
	b int64
}

type handler struct {
	elements []chan element
	err error
}


// give a file, return multiple channel, channel size is max to 1026
func readParallel(fileName string, parallelNum int, divideNum int) ([]handler, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
		return nil, err
	}



	size := fi.Size()
	if size < parallelMinSize {
		parallelNum = 1
	}

	split := size/int64(parallelNum)
	seekPositions := make([]int64, parallelNum+1, parallelNum+1)
	for i := 0; i < parallelNum; i++ {
		seekPositions[i] = int64(i) * split
		if i > 0 {
			seekPositions[parallelNum], err = alignToRows(f, seekPositions[parallelNum])
			if err != nil {
				return nil, err
			}
		}
	}
	seekPositions[parallelNum] = math.MaxInt64



	handlers := make([]handler, parallelNum, parallelNum)

	for i := 0; i < parallelNum; i++ {
		go func() {
			handlers[i].elements = make([]chan<-element, divideNum)
			defer func() {
				for _, channel := range handlers[i].elements {
					close(channel)
				}
			} ()

			handlers[i].err = oneStream(fileName, handlers[i].elements, seekPositions[i], seekPositions[i+1] - seekPositions[i], divideNum)
		}()
	}
	return handlers, nil
}

func alignToRows(f *os.File, start int64) (int64, error) {
	_, err := f.Seek(start, 0)
	if err != nil {
		return 0, err
	}
	buffer := make([]byte, fileInputBufferSize)
	_, err = f.Read(buffer)
	if err != nil {
		return 0, err
	}
	offset := int64(0)
	for buffer[offset] != '\n' {
		offset++
	}
	return start + offset + 1, nil
}

func hash(num int64, devide int) int {
	return int(num%int64(devide))
}

const stateNumber = 1
const stateNonNumber = 0

func oneStream(fileName string, elements []chan<- element, seekStart int64, maxRead int64, devideNum int) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Seek(seekStart, 0)
	if err != nil {
		return err
	}

	buffer := make([]byte, fileInputBufferSize)


	state := stateNonNumber
	number := make([]byte, 0, 64)
	var first int64
	var second int64
	var numbersNumInRow int
	leftRead := maxRead
	for leftRead > 0 {
		if leftRead < fileInputBufferSize {
			buffer = buffer[:leftRead]
		}
		nRead, err := f.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}
		leftRead -= int64(nRead)
		for _, b := range buffer {
			switch state {
			case stateNonNumber:
				if !(b >= '0' && b <= '9') {
					return fmt.Errorf("unknonw input format: number expected")
				}
				number = append(number, b)
				state = stateNumber
			case stateNumber:
				if b >= '0' && b <= '9' {
					number = append(number, b)
				} else if b == '\t' {
					if numbersNumInRow != 0 {
						return fmt.Errorf("multiple numbers in a row")
					}
					first, err = strconv.ParseInt(string(number), 0, 64)
					if err != nil {
						return err
					}
					numbersNumInRow++
					number = number[:0]
					state = stateNonNumber
				} else if b == '\n' {
					if numbersNumInRow != 1 {
						return fmt.Errorf("numbers in a row not 2")
					}
					second, err = strconv.ParseInt(string(number), 0, 64)
					if err != nil {
						return err
					}
					elements[hash(first, devideNum)]<-element{a: first, b: second}
					number = number[:0]
					numbersNumInRow = 0
					state = stateNonNumber
				} else {
					return fmt.Errorf("unknonw input format")
				}
			}
		}
	}

	return nil
}
