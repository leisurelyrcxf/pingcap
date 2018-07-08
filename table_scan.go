package pingcap

import (
	"os"
	"math"
	"io"
	"fmt"
	"strconv"
)

const pageSize = 1024 * 4 //4kb
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
	seekPositions := make([]int64, parallelNum+1)
	for i := 0; i < parallelNum; i++ {
		seekPositions[i] = int64(i) * split
		if i > 0 {
			seekPositions[i], err = alignToNewLine(f, seekPositions[i])
			if err != nil {
				return nil, err
			}
		}
	}
	seekPositions[parallelNum] = math.MaxInt64



	handlers := make([]handler, parallelNum)

	for i := 0; i < parallelNum; i++ {
		idx := i
		handlers[i].elements = make([]chan element, divideNum)
		for j := 0; j < divideNum; j++ {
			handlers[i].elements[j] = make(chan element, pageSize)
		}
		go func() {
			defer func() {
				for _, channel := range handlers[idx].elements {
					close(channel)
				}
			} ()

			handlers[idx].err = oneStream(fileName, handlers[idx].elements, seekPositions[idx], seekPositions[idx+1] - seekPositions[idx], divideNum)
		}()
	}
	return handlers, nil
}

// align to new line
func alignToNewLine(f *os.File, start int64) (int64, error) {
	_, err := f.Seek(start, 0)
	if err != nil {
		return 0, err
	}
	buffer := make([]byte, pageSize)
	_, err = f.Read(buffer)
	if err != nil {
		return 0, err
	}
	offset := int64(0)
	for offset < pageSize && buffer[offset] != '\n' {
		offset++
	}
	if offset == pageSize {
		return start, fmt.Errorf("can't find '\n'")
	}
	return start + offset + 1, nil
}

func hash(num int64, devide int) int {
	return int(num%int64(devide))
}

const stateNumber = 1
const stateNonNumber = 0

func oneStream(fileName string, elements []chan element, seekStart int64, maxRead int64, divideNum int) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Seek(seekStart, 0)
	if err != nil {
		return err
	}

	buffer := make([]byte, pageSize)


	state := stateNonNumber
	number := make([]byte, 0, 64)
	var first int64
	var second int64
	var numbersNumInRow int
	var eofMet bool
	leftRead := maxRead
	for leftRead > 0 && !eofMet {
		if leftRead < pageSize {
			buffer = buffer[:leftRead]
		}
		nRead, err := f.Read(buffer)
		if err != nil {
			if err != io.EOF {
				return err
			}
			eofMet = true
		}
		leftRead -= int64(nRead)
		for i := 0; i < nRead; i++ {
			b := buffer[i]
			switch state {
			case stateNonNumber:
				if !(b >= '0' && b <= '9') {
					return fmt.Errorf("unknown input format: number expected")
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
						//fmt.Println(number)
						//fmt.Println(first)
						//fmt.Println(second)
						//fmt.Println(buffer)
						return fmt.Errorf("numbers in a row less than 2")
					}
					second, err = strconv.ParseInt(string(number), 0, 64)
					if err != nil {
						return err
					}
					elements[hash(first, divideNum)]<-element{a: first, b: second}
					number = number[:0]
					numbersNumInRow = 0
					state = stateNonNumber
				} else {
					return fmt.Errorf("unknown input character '%v'", b)
				}
			}
		}
	}

	return nil
}
