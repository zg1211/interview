package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/zg1211/interview/meta"
)

var (
	input       = "tasks.csv"
	attempts    = 3
	concurrency = 10
)

const (
	StatusSuccess = "succeeded"
	StatusFailed  = "failed"

	ColADID       = "ad_id"
	ColCreativeID = "new_creative_id"
	ColStatus     = "status"
	ColAttempts   = "attempts"
)

func init() {
	flag.StringVar(&input, "i", input, "input task file path")
	flag.IntVar(&attempts, "a", attempts, "number of attempts")
	flag.IntVar(&concurrency, "c", concurrency, "concurrency")
	flag.Parse()
}

func main() {
	f, err := os.Open(input)
	if err != nil {
		log.Fatal(err)
	}
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	metaClient := meta.NewClient()
	cc := make(chan struct{}, concurrency)
	wg := sync.WaitGroup{}
	if len(records) < 1 {
		log.Fatal("no records found")
	}
	colADIDIndex, colCreativeIDIndex := -1, -1
	for i, col := range records[0] {
		if col == ColADID {
			colADIDIndex = i
		}
		if col == ColCreativeID {
			colCreativeIDIndex = i
		}
	}
	if colADIDIndex == -1 {
		log.Fatalf("no %s column found", ColADID)
	}
	if colCreativeIDIndex == -1 {
		log.Fatalf("no %s column found", ColCreativeID)
	}
	outputs := make([][]string, len(records))
	for i, record := range records[1:] {
		wg.Add(1)
		go func() {
			cc <- struct{}{}
			defer wg.Done()
			defer func() { <-cc }()
			status := StatusSuccess
			a, err := updateADCreativeProc{
				metaClient: metaClient,
				adID:       record[colADIDIndex],
				creativeID: record[colCreativeIDIndex],
				attempts:   attempts,
			}.do()
			if err != nil {
				status = StatusFailed
			}
			outputs[i] = []string{record[colADIDIndex], record[colCreativeIDIndex], status, fmt.Sprintf("%d", a)}
		}()
	}
	wg.Wait()
	rf, err := os.Create(fmt.Sprintf("%s_results.csv", time.Now().Format("20060102150405")))
	if err != nil {
		log.Fatal(err)
	}
	writer := csv.NewWriter(rf)
	defer writer.Flush()
	if err = writer.Write([]string{ColADID, ColCreativeID, ColStatus, ColAttempts}); err != nil {
		log.Fatal(err)
	}
	for _, output := range outputs {
		if err = writer.Write(output); err != nil {
			log.Fatalf("write csv error: %s", err)
		}
	}
}

type updateADCreativeProc struct {
	metaClient *meta.Client
	adID       string
	creativeID string
	attempts   int
}

func (p updateADCreativeProc) do() (int, error) {
	var (
		i   int
		err error
	)
	for i = range p.attempts {
		err = p.metaClient.UpdateADCreative(p.adID, p.creativeID)
		switch {
		case err == nil:
			return i + 1, nil
		case errors.Is(err, meta.ErrInvalidADID) || errors.Is(err, meta.ErrInvalidCreativeID):
			return i + 1, err
		case errors.Is(err, meta.ErrRateLimited):
			time.Sleep(time.Millisecond * 100)
		}
	}
	return p.attempts, err
}
