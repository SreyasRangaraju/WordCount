package main

import (
  "fmt"
  "os"
  "strconv"
  "os/exec"
  "io/ioutil"
  "strings"
  "sort"
  "bytes"
  "hash/fnv"
  "sync"
)
type SMap struct {
  m map[string]int
  s []string
}

type mWord struct {
  word string
  count int
}

func (sMap *SMap) Len() int {
  return len(sMap.m)
}

func (sMap *SMap) Less(i, j int) bool {
  if sMap.m[sMap.s[i]] == sMap.m[sMap.s[j]]{
    return sMap.s[i] < sMap.s[j]
  }
  return sMap.m[sMap.s[i]] > sMap.m[sMap.s[j]]
}

func (sMap *SMap) Swap(i, j int) {
  sMap.s[i], sMap.s[j] = sMap.s[j], sMap.s[i]
}

func sortKeys(m map[string]int) []string {
  sMap := new(SMap)
  sMap.m = m
  sMap.s = make([]string, len(m))
  i := 0
  for key, _ := range m {
    sMap.s[i] = key
    i++
  }
  sort.Sort(sMap)
  return sMap.s
}

func hash(sh string) uint32 {
        hs := fnv.New32a()
        hs.Write([]byte(sh))
        return hs.Sum32()
}

func mapper(words []string, nReduce int, mCount int){
  
  rFiles := [][]mWord{}
  for i := 0; i < nReduce; i++ {
    rFiles = append(rFiles,[]mWord{})
  }
  
  for _, word := range words{
    hNum := hash(word) % uint32(nReduce)
    rFiles[hNum] = append(rFiles[hNum][:],mWord{word,1})
  }
  
  var buffer bytes.Buffer
  for i := 0; i < nReduce; i++{
    for j:= 0; j < len(rFiles[i]);j++{
      buffer.WriteString(fmt.Sprintf(rFiles[i][j].word + " " + strconv.Itoa(rFiles[i][j].count) + "\n"))
    }
    ioutil.WriteFile("map" + strconv.Itoa(mCount) + "_" + strconv.Itoa(i) + ".txt", []byte(buffer.String()), 0644)
    buffer.Reset()
  }
}

func reducer(rNum int, nMap int){
  var rFile []byte
  for i := 0;i < nMap; i++{
    tempR, _ := ioutil.ReadFile("map" + strconv.Itoa(i) + "_" + strconv.Itoa(rNum) + ".txt")
    rFile = append(rFile, tempR...)
  }
  rString := string(rFile[:])
  counts := map[string]int{}
  var buffer bytes.Buffer

  words := strings.Fields(rString)
  for i := 0; i < len(words); i += 2{
    tempCount, _ := strconv.Atoi(words[i+1])
    counts[words[i]] += tempCount
  }

  for _, res := range sortKeys(counts) {
    buffer.WriteString(fmt.Sprintf(res + " %d\n", counts[res]))
  }
  ioutil.WriteFile("red" + strconv.Itoa(rNum) + ".txt",[]byte(buffer.String()),0644)
}

func combiner(nReduce int, outFile string){
  var rFile []byte
  for i := 0;i < nReduce; i++{
    tempR, _ := ioutil.ReadFile("red" + strconv.Itoa(i) + ".txt")
    rFile = append(rFile, tempR...)
  }
  rString := string(rFile[:])
  counts := map[string]int{}
  var buffer bytes.Buffer

  words := strings.Fields(rString)
  for i := 0; i < len(words); i += 2{
    tempCount, _ := strconv.Atoi(words[i+1])
    counts[words[i]] += tempCount
  }

  for _, res := range sortKeys(counts) {
    buffer.WriteString(fmt.Sprintf(res + " %d\n", counts[res]))
  }
  ioutil.WriteFile(outFile,[]byte(buffer.String()),0644)
}

func cleanFiles(){
  mapRemove := "rm map*.txt"
  redRemove := "rm red*.txt"
  exec.Command("bash","-c",mapRemove).Output()
  exec.Command("bash","-c",redRemove).Output()
}

func WordCount_MR_DMP(inFile string, outFile string, nMap int, nReduce int) {

}

func WordCount_MR_SMP(inFile string, outFile string, nMap int, nReduce int) {
  var mapGroup sync.WaitGroup
  var redGroup sync.WaitGroup

  godfather, _ := ioutil.ReadFile(inFile)
  strGF := string(godfather)
  words := strings.Fields(strGF)

  for i := 0; i < nMap - 1; i++ {
    mapGroup.Add(1)
    go func(i int){
      mapper(words[len(words)/nMap * i:len(words)/nMap * (i + 1)], nReduce, i)
      defer mapGroup.Done()
    }(i)
  }
  mapGroup.Add(1)
  go func(){
      mapper(words[len(words)/nMap * (nMap - 1):], nReduce, nMap - 1)
      defer mapGroup.Done()
  }()

  mapGroup.Wait()  
  
  for i := 0; i < nReduce; i++ {
    redGroup.Add(1)
    go func(i int){
      reducer(i, nMap)
      defer redGroup.Done()
    }(i)
  }
  
  redGroup.Wait()

  for i := 0; i < nReduce; i++ {
    combiner(nReduce, outFile)
  }

  cleanFiles()
}

func WordCount_MR_S(inFile string, outFile string, nMap int, nReduce int) {
  godfather, _ := ioutil.ReadFile(inFile)
  strGF := string(godfather)
  words := strings.Fields(strGF)

  for i := 0; i < nMap - 1; i++ {
    mapper(words[len(words)/nMap * i:len(words)/nMap * (i + 1)], nReduce, i)
  }
  mapper(words[len(words)/nMap * (nMap - 1):], nReduce, nMap - 1)
  
  for i := 0; i < nReduce; i++ {
    reducer(i, nMap)
  }

  for i := 0; i < nReduce; i++ {
    combiner(nReduce, outFile)
  }

  cleanFiles()
}

func WordCount_GO(inFile string, outFile string) {
  godfather, _ := ioutil.ReadFile(inFile)
  strGF := string(godfather)
  counts := map[string]int{}
  var buffer bytes.Buffer

  words := strings.Fields(strGF)
  for _, word := range words{
    counts[word] += 1
  }

  for _, res := range sortKeys(counts) {
    buffer.WriteString(fmt.Sprintf(res + " %d\n", counts[res]))
  }
  ioutil.WriteFile(outFile,[]byte(buffer.String()),0644)
}

func WordCount_UNIX(inFile string, outFile string) {
  exec.Command("bash", "-C", "wordcount.sh", inFile, outFile).Run()
}

func main() {
	if len(os.Args) < 4 || len(os.Args) > 6{
		fmt.Printf("%s:\n\tUsage: bin/wordcount run-mode inFile outFile <NMap> <NReduce>\n", os.Args[0])
		fmt.Printf("\tRun Modes:\n\t\t1 Unix Pipeline\n\t\t2 Simple wordcount in GO\n\t\t3 MapReduce Sequential\n\t\t4 MapReduce SMP\n\t\t5 MapReduce DMP\n")
		return
	}

	switch os.Args[1] {
  case "1":
    WordCount_UNIX(os.Args[2], os.Args[3])
  case "2":
    WordCount_GO(os.Args[2], os.Args[3])
  case "3":
    nMap, _ := strconv.Atoi(os.Args[4])
    nReduce, _ := strconv.Atoi(os.Args[5])
    WordCount_MR_S(os.Args[2], os.Args[3], nMap, nReduce)
  case "4":
    nMap, _ := strconv.Atoi(os.Args[4])
    nReduce, _ := strconv.Atoi(os.Args[5])
    WordCount_MR_SMP(os.Args[2], os.Args[3], nMap, nReduce)
  case "5":
    nMap, _ := strconv.Atoi(os.Args[4])
    nReduce, _ := strconv.Atoi(os.Args[5])
    WordCount_MR_DMP(os.Args[2], os.Args[3], nMap, nReduce)
  default:
    fmt.Printf("Unknown run-mode\n")	
  }
}
