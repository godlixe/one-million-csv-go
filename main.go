package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func openDBConnection() (*sql.DB, error) {
	log.Println("Open DB Connection")
	const dbConnString = "root@/test"

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

const dbMaxIdleConns = 4
const dbMaxConns = 5
const totalWorker = 5
const csvFile = "majestic_million.csv"

var dataHeaders = make([]string, 0)

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

func dispatchWorkers(db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorker; workerIndex++ {
		// fmt.Println("workerIndex -> ", workerIndex)
		go func(workerIndex int, db *sql.DB, jobs <-chan []interface{}, wg *sync.WaitGroup) {
			counter := 0
			var sb strings.Builder
			// var query string
			sb.WriteString("INSERT INTO domain (GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,PrevGlobalRank,PrevTldRank,PrevRefSubNets,PrevRefIPs) VALUES ")
			for job := range jobs {
				sb.WriteString("(")
				ct := 0
				for _, str := range job {
					ct++
					sb.WriteString("\"")
					sb.WriteString(str.(string))
					sb.WriteString("\"")
					if ct != len(job) {
						sb.WriteString(",")
					}
				}
				// if ct <= 12 {
				// 	sb.WriteString(",")
				// }
				for ct < 12 {
					ct++
					sb.WriteString("-1,")
				}
				sb.WriteString(")\n")
				sb.WriteString(",")
				counter++
				// fmt.Println(sb.String())
				if counter == 5000 {
					doTheJob(workerIndex, counter, db, sb)
					counter = 0
					sb.Reset()
					sb.WriteString("INSERT INTO domain (GlobalRank,TldRank,Domain,TLD,RefSubNets,RefIPs,IDN_Domain,IDN_TLD,PrevGlobalRank,PrevTldRank,PrevRefSubNets,PrevRefIPs) VALUES ")
				}
				wg.Done()
			}
		}(workerIndex, db, jobs, wg)
	}
}

func readCsvFilePerLineThenSendToWorker(csvReader *csv.Reader, jobs chan<- []interface{}, wg *sync.WaitGroup) {
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}
func doTheJob(workerIndex, counter int, db *sql.DB, str strings.Builder) {
	for {
		var outerError error
		func(outerError *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerError = fmt.Errorf("%v", err)
				}
			}()
			// query := "
			conn, err := db.Conn(context.Background())
			if err != nil {
				panic(err)
			}
			// fmt.Println(str.String())
			_, err = conn.ExecContext(context.Background(), strings.TrimSuffix(str.String(), ","))
			if err != nil {
				log.Fatal(err.Error())
			}

			err = conn.Close()
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerError)
		if outerError == nil {
			break
		}
	}

	// if counter%100 == 0 {
	// 	log.Println("=> worker", workerIndex, "inserted", counter, "data")
	// }
}

// func generateQuestionsMark(n int) []string {
// 	s := make([]string, 0)
// 	for i := 0; i < n; i++ {
// 		s = append(s, "?")
// 	}
// 	return s
// }
func main() {
	start := time.Now()

	db, err := openDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	jobs := make(chan []interface{})
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvFilePerLineThenSendToWorker(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)
	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}
