package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/valyala/fasthttp"
)

var (
	totalPatents     int32 = 0
	processedPatents int32 = 0
	rateLimit        int32 = 20 // Initial rate limit
	concurrencyLevel int32 = 15
)

func fetchAssignees(patentID string, csvWriter *csv.Writer, wg *sync.WaitGroup, pool *ants.Pool, counter *uint64) {
	defer wg.Done()

	for {
		url := fmt.Sprintf("https://api.patentsview.org/assignees/query?q={\"_eq\":{\"patent_id\":\"%s\"}}", patentID)
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)

		req.SetRequestURI(url)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		// Limit the rate
		for atomic.LoadInt32(&rateLimit) <= 0 {
			time.Sleep(time.Second)
		}

		if err := fasthttp.Do(req, resp); err != nil {
			fmt.Printf("Error fetching patent_id %s: %s\n", patentID, err)
			return
		}

		if resp.StatusCode() == fasthttp.StatusOK {
			atomic.AddInt32(&processedPatents, 1)
			fmt.Printf("Progress: %d/%d\n", atomic.LoadInt32(&processedPatents), atomic.LoadInt32(&totalPatents))

			var result map[string]interface{}
			if err := json.Unmarshal(resp.Body(), &result); err != nil {
				fmt.Printf("Error parsing JSON for patent_id %s: %s\n", patentID, err)
				return
			}

			assigneesData, ok := result["assignees"].([]interface{})
			if !ok || len(assigneesData) == 0 {
				// No assignees or assignees field is not an array
				csvWriter.Write([]string{patentID, "", ""})
				return
			}

			for _, assignee := range assigneesData {
				assigneeData, ok := assignee.(map[string]interface{})
				if !ok {
					fmt.Printf("Error parsing assignee data for patent_id %s\n", patentID)
					continue
				}

				assigneeID, _ := assigneeData["assignee_id"].(string)
				assigneeOrg, _ := assigneeData["assignee_organization"].(string)

				csvWriter.Write([]string{patentID, assigneeID, assigneeOrg})
			}

			// Reset rate limit
			atomic.StoreInt32(&rateLimit, 15)

			// Increment the counter
			atomic.AddUint64(counter, 1)

			// Check if 1000 requests have been processed
			if *counter%1000 == 0 {
				fmt.Println("Time taken for last 1000 requests:", time.Since(time.Now()))
			}
			break
		} else if resp.StatusCode() == fasthttp.StatusTooManyRequests {
			fmt.Printf("Rate limit exceeded. Retrying in 10 seconds...\n")
			atomic.StoreInt32(&rateLimit, 0) // Set rate limit to 0 temporarily
			time.Sleep(10 * time.Second)     // Wait for 10 seconds before retrying
		} else {
			fmt.Printf("Error fetching patent_id %s: %d\n", patentID, resp.StatusCode())
			return
		}
	}
}

func main() {
	patentIDsPath := "./patent_ids.txt"
	f, err := os.Open(patentIDsPath)
	if err != nil {
		fmt.Println("Error opening patent IDs file:", err)
		return
	}
	defer f.Close()

	var patentIDs []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		patentIDs = append(patentIDs, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error scanning patent IDs file:", err)
		return
	}

	atomic.StoreInt32(&totalPatents, int32(len(patentIDs))) // Set total number of patents

	mappingFilePath := "./patent_id_assignee_mapping.csv"
	f, err = os.Create(mappingFilePath)
	if err != nil {
		fmt.Println("Error creating mapping file:", err)
		return
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)
	csvWriter.Write([]string{"patent_id", "assignee_id", "assignee_organization"})

	wg := sync.WaitGroup{}
	pool, _ := ants.NewPool(int(concurrencyLevel))
	defer pool.Release()

	var counter uint64 // Counter to track the number of requests processed

	for _, patentID := range patentIDs {
		wg.Add(1)
		_ = pool.Submit(func() {
			fetchAssignees(patentID, csvWriter, &wg, pool, &counter)
		})
	}

	wg.Wait()
	csvWriter.Flush()
	fmt.Println("Finished processing all patent IDs.")
}
