package http

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type GetOrderMsg struct {
	orderID string
}

type OrderMsgHasher struct{}

func (o *OrderMsgHasher) ComputeHash(data interface{}) int {
	val := data.(GetOrderMsg)
	h := fnv.New32a()
	h.Write([]byte(val.orderID))
	return int(h.Sum32())
}

func (g GetOrderMsg) GetMethod(conf HTTPSinkConf) string {
	return "GET"
}

func (g GetOrderMsg) GetPayload() []byte {
	return nil
}

func (g GetOrderMsg) IsSidelined() bool {
	return false
}

func (g GetOrderMsg) Sideline() {

}

func (g GetOrderMsg) GetHeaders(conf HTTPSinkConf) map[string]string {
	header := map[string]string{
		"X_CLIENT_ID": "TEST",
	}
	return header
}

func (g GetOrderMsg) GetURLPath() string {
	return "/path/" + g.orderID
}

func (g GetOrderMsg) GetURL(endpoint string) string {
	return endpoint + "/path/" + g.orderID
}

func (g GetOrderMsg) GetDebugPath() string {
	return "/path/" + g.orderID
}

func (g GetOrderMsg) BatchURL(msgs []interface{}, endpoint string, version int) string {
	return endpoint + "/batch" + g.orderID
}

func (g GetOrderMsg) BatchPayload(msgs []interface{}, version int) []byte {
	return []byte("batch payload")
}

// Use a custom hook that records retry timestamps
type retryRecord struct {
	attempts []time.Time
}

type testHook struct {
	record *retryRecord
}

func (t *testHook) PreHTTPCall(msg interface{}) {
	t.record.attempts = append(t.record.attempts, time.Now())
}

func (t *testHook) PostHTTPCall(msg interface{}, success bool) {
	log.Println("PostHTTPCall:", msg, success)
}

type FileSource struct {
	inputFilePath string
}

func GetFileSource(path string) *FileSource {
	return &FileSource{path}
}

func (f *FileSource) Generate(out chan<- interface{}) {

	ordersBytes, err := ioutil.ReadFile(f.inputFilePath)
	if err != nil {
		log.Fatalf("Failed to read file %s, %v", f.inputFilePath, err)
		return
	}

	ordersString := strings.TrimSpace(string(ordersBytes))
	orders := strings.Split(ordersString, "\n")
	for _, orderID := range orders {
		msg := GetOrderMsg{orderID}
		out <- msg
	}

}

func (f *FileSource) Stop() {

}

func parseConf(path string) HTTPSinkConf {

	raw, err := ioutil.ReadFile(path)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	var conf HTTPSinkConf
	json.Unmarshal(raw, &conf)

	return conf
}

func TestHTTPSinkWithRetryBackoff(t *testing.T) {
	log.Println("running test TestHTTPSinkWithRetryBackoff")

	// Setup a test HTTP server that fails with 500 status code
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	defer server.Close()

	// Override conf endpoint to use test server
	conf := parseConf("sink_test.json")
	conf.Endpoint = server.URL

	record := &retryRecord{attempts: make([]time.Time, 0)}

	hook := &testHook{record: record}
	sink := GetHTTPSink(10, conf)
	sink.RegisterHook(hook)
	msg := GetOrderMsg{"123"}

	startTime := time.Now()
	sink.Consume(msg, 3, nil)
	totalTime := time.Since(startTime)

	// Validate that the hook was called
	if len(record.attempts) < 1 {
		t.Errorf("Expected at least 1 hook call, got %d", len(record.attempts))
	}

	// Validate that the total execution time indicates multiple retries with backoff
	// With 3 retries and exponential backoff starting at ~300ms, total time should be > 1 second
	expectedMinTime := 1 * time.Second
	if totalTime < expectedMinTime {
		t.Errorf("Expected total execution time to be at least %v (indicating retries with backoff), but got %v", expectedMinTime, totalTime)
	}

	log.Printf("Total execution time: %v (indicating successful retry with exponential backoff)", totalTime)
}
