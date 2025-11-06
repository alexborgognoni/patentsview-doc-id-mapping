# PatentsView Document ID Mapping

A concurrent Go program that fetches patent-to-assignee mappings from the PatentsView API with dynamic rate limiting and parallel processing.

## About

This project was developed to support the master thesis: ["Decoding AI Advantage"](https://ulb-dok.uibk.ac.at/ulbtirolhs/content/pageview/10116699) by Noah Zieser and Thomas Lindner at the University of Innsbruck.

## Overview

This tool reads a list of patent IDs and queries the [PatentsView API](https://api.patentsview.org) to retrieve assignee information for each patent. It outputs the results to a CSV file containing patent IDs, assignee IDs, and assignee organizations.

## Features

- Dynamic rate limiting (up to 20 req/sec)
- Concurrent request processing with goroutine pooling
- Automatic retry handling for rate limit responses
- Progress tracking
- CSV output format

## Usage

1. Place your patent IDs in `patent_ids.txt` (one ID per line)
2. Run the program:
   ```bash
   go run main.go
   ```
3. The results will be saved to `patent_id_assignee_mapping.csv`

## Requirements

- Go 1.22.1 or higher
- Dependencies (managed via go.mod):
  - github.com/panjf2000/ants/v2 (goroutine pool)
  - github.com/valyala/fasthttp (HTTP client)
