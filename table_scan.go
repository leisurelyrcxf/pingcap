package pingcap

import (
	"os"
	"math"
	"io"
	"fmt"
	"strconv"
	//"runtime"
	"io/ioutil"
	"strings"
)

const pageSize = 1024 * 4 //4kb

const parallelReadMinSize = 1024*1024
var defaultParallelReadNum = 2 //runtime.NumCPU()
var defaultInMemoryDivide = 2 //runtime.NumCPU()

// change this according to your application
//var maxAvailableMemory = 1024*1024*256 // 256 MB
var maxAvailableMemory int64 = 1024*1024*48

// estimated value, cause for every record in S, in the worst case
// there should be two hash table records using that value
const memoryConflateRate = 5

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
func readParallel(fileName string) (handlers []handler, parallelReadNum, divideNum, inMemoryDivideNum int, err error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
		return nil, 0, 0, 0, err
	}
	size := fi.Size()
	if size < parallelReadMinSize {
		parallelReadNum = 1
		divideNum = defaultInMemoryDivide
		inMemoryDivideNum = defaultInMemoryDivide
	} else if size*memoryConflateRate < maxAvailableMemory {
		// all can be put into memory
		parallelReadNum = defaultParallelReadNum
		divideNum = defaultInMemoryDivide
		inMemoryDivideNum = defaultInMemoryDivide
	} else {
		parallelReadNum = defaultParallelReadNum
		divideNum = int(size*int64(memoryConflateRate)/int64(maxAvailableMemory)+1)*defaultInMemoryDivide
		// in this case, only first defaultInMemoryDivide divisions will be in memory
		// others will be flushed onto disks
		inMemoryDivideNum = defaultInMemoryDivide
	}

	split := size/int64(parallelReadNum)
	seekPositions := make([]int64, parallelReadNum+1)
	seekPositions[parallelReadNum] = math.MaxInt64
	for i := 0; i < parallelReadNum; i++ {
		seekPositions[i] = int64(i) * split
		if i > 0 {
			seekPositions[i], err = alignToNewLine(f, seekPositions[i])
			if err != nil {
				return nil, 0, 0, 0, err
			}
		}
	}



	handlers = make([]handler, parallelReadNum)

	for i := 0; i < parallelReadNum; i++ {
		idx := i
		handlers[i].elements = make([]chan element, inMemoryDivideNum)
		for j := 0; j < inMemoryDivideNum; j++ {
			handlers[i].elements[j] = make(chan element, pageSize)
		}
		go func() {
			defer func() {
				for _, channel := range handlers[idx].elements {
					close(channel)
				}
			} ()

			handlers[idx].err = oneStream(fileName, handlers[idx].elements, seekPositions[idx], seekPositions[idx+1] - seekPositions[idx], divideNum, inMemoryDivideNum)
		}()
	}
	return handlers, parallelReadNum, divideNum, inMemoryDivideNum, nil
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

const stateNumber = 1
const stateNonNumber = 0

func oneStream(fileName string, elements []chan element, seekStart int64, maxRead int64, divideNum, inMemoryDivideNum int) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Seek(seekStart, 0)
	if err != nil {
		return err
	}

	var fwHandlers []*os.File
	var fwOffsets []int
	var fwBuffers [][]byte
	if inMemoryDivideNum < divideNum {
		fwHandlers = make([]*os.File, divideNum - inMemoryDivideNum)
		fwOffsets = make([]int, divideNum - inMemoryDivideNum)
		fwBuffers = make([][]byte, divideNum - inMemoryDivideNum)
		for i := inMemoryDivideNum; i < divideNum; i++ {
			divideIdx := i
			divideRelativeIdx := divideIdx - inMemoryDivideNum
			tmpFileName := getTmpFileName(fileName, divideIdx, seekStart)
			fwHandlers[divideRelativeIdx], err = os.OpenFile(tmpFileName, os.O_WRONLY | os.O_CREATE, 0755)
			if err != nil {
				return err
			}
			defer fwHandlers[divideRelativeIdx].Close()
			fwOffsets[divideRelativeIdx] = 0
			fwBuffers[divideRelativeIdx] = make([]byte, pageSize)
		}
	}



	return readFile(f, maxRead, func(e element) error {
		hashed := hash(e.a, divideNum)
		if hashed < inMemoryDivideNum {
			elements[hashed]<-e
		} else {
			divideRelativeIdx := hashed - inMemoryDivideNum
			var err error
			fwOffsets[divideRelativeIdx], err = writeBuffered(fwHandlers[divideRelativeIdx], fwBuffers[divideRelativeIdx],
				fwOffsets[divideRelativeIdx], pageSize, e)
			if err != nil {
				return err
			}

		}
		return nil
	}, func() error {
		if divideNum > inMemoryDivideNum {
			for i := inMemoryDivideNum; i < divideNum; i++ {
				divideIdx := i
				divideRelativeIdx := divideIdx - inMemoryDivideNum
				_, err := fwHandlers[divideRelativeIdx].Write(fwBuffers[divideRelativeIdx][:fwOffsets[divideRelativeIdx]])
				if err != nil {
					return err
				}
				err = fwHandlers[divideRelativeIdx].Sync()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func readFile(f *os.File, maxRead int64, onElementFound func(element) error, onEnd func() error) error {
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
						return fmt.Errorf("numbers in a row less than 2")
					}
					second, err = strconv.ParseInt(string(number), 0, 64)
					if err != nil {
						return err
					}
					err = onElementFound(element{a:first, b:second})
					if err != nil {
						return err
					}
					number = number[:0]
					numbersNumInRow = 0
					state = stateNonNumber
				} else {
					return fmt.Errorf("unknown input character '%v'", b)
				}
			}
		}
		leftRead -= int64(nRead)
	}
	return onEnd()
}

func hash(num int64, divide int) int {
	return int(num%int64(divide))
}

func getTmpFileName(fileName string, divideIdx int, seekStart int64) string {
	return getTmpFilePrefix(fileName, divideIdx) + strconv.FormatInt(seekStart, 10) +  ".tmp"
}

func getTmpFileNames(fileName string, divideIdx int) (files []string, err error) {
	tmpIdx := strings.LastIndex(fileName, "/")
	var fileDir string
	var prefix string
	if tmpIdx == -1 {
		fileDir = "."
		prefix = getTmpFilePrefix(fileName, divideIdx)
	} else {
		fileDir = fileName[:tmpIdx]
		prefix = getTmpFilePrefix(fileName[tmpIdx+1:], divideIdx)
	}
	dir, err := ioutil.ReadDir(fileDir)
	if err != nil {
		return nil, err
	}

	PthSep := string(os.PathSeparator)

	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasPrefix(fi.Name(), prefix) {
			files = append(files, fileDir+PthSep+fi.Name())
		}
	}

	return files, nil
}

func getTmpFilePrefix(fileName string, divideIdx int) string {
	return fileName + "_divide" + strconv.Itoa(divideIdx) + "_pid" + strconv.Itoa(os.Getpid()) + "_seek"
}

func writeBuffered(fw *os.File, buffer []byte, offset int, maxBufferSize int, e element) (newOffset int, err error) {
	row := []byte(elementToRow(e))
	for _, b := range row {
		buffer[offset] = b
		offset++
		if offset == maxBufferSize {
			_, err := fw.Write(buffer)
			if err != nil {
				return 0, err
			}
			err = fw.Sync()
			if err != nil {
				return 0, err
			}
			offset = 0
		}
	}
	return offset, nil
}

func elementToRow(e element) string {
	return strconv.FormatInt(e.a, 10) + "\t" + strconv.FormatInt(e.b, 10) + "\n"
}
