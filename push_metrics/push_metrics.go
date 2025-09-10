package push_metrics

import (
	"bytes"
	"log"
	"net/http"
)

func Push_metrics(metricStr string) {

	url := "https://xxxxxxxxxxxxxxxxxxxx/insert/0/prometheus/api/v1/import/prometheus"
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(metricStr))
	if err != nil {
		log.Fatalf("failed to create HTTP request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Fatalf("received non-2XX response: %s", resp.Status)
	}

	//log.Println("Data successfully sent to remote storage")
}
