package matsuri

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	models "github.com/alceccentric/matsurihi-cron/models"
	resty "github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
)

func setupTestServer(_ *testing.T, handler http.HandlerFunc) (*httptest.Server, *MatsurihiMeClient) {
	server := httptest.NewServer(handler)
	client := NewMatsurihiMeClient(server.URL)
	client.httpClient = resty.New() // Use default resty client for local server
	return server, client
}

func TestGetEvents(t *testing.T) {
	expected := []models.Event{
		{Id: 1, Name: "Test Event", Type: 2},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	events, err := client.GetEvents(nil)
	assert.NoError(t, err)
	assert.Equal(t, expected, events)
}

func TestGetEvent(t *testing.T) {
	expected := models.Event{Id: 42, Name: "Event42", Type: 3}
	handler := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	event, err := client.GetEvent(42)
	assert.NoError(t, err)
	assert.Equal(t, expected, event)
}

func TestGetEventRankingBorders(t *testing.T) {
	expected := models.EventRankingBorders{
		EventPoint: []int{100, 2500, 5000},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	borders, err := client.GetEventRankingBorders(1)
	assert.NoError(t, err)
	assert.Equal(t, expected, borders)
}

func TestGetEventRankingLogs(t *testing.T) {
	expected := []models.EventRankingLog{
		{Rank: 100, Data: []struct {
			Score        int       `json:"score"`
			AggregatedAt time.Time `json:"aggregatedAt"`
		}{
			{Score: 12345, AggregatedAt: time.Now()},
		}},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	logs, err := client.GetEventRankingLogs(1, models.EventPoint, 100, nil)
	assert.NoError(t, err)
	assert.Equal(t, expected[0].Rank, logs[0].Rank)
	assert.Equal(t, expected[0].Data[0].Score, logs[0].Data[0].Score)
}

func TestSendGetRequest_ErrorStatus(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	var v interface{}
	err := client.sendGetRequest(server.URL, nil, nil, &v)
	assert.Error(t, err)
}

func TestSendGetRequest_InvalidJSON(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	var v interface{}
	err := client.sendGetRequest(server.URL, nil, nil, &v)
	assert.Error(t, err)
}

func TestGetEvents_InvalidJSON(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	_, err := client.GetEvents(nil)
	assert.Error(t, err)
}

func TestGetEvent_NotFound(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	_, err := client.GetEvent(999)
	assert.Error(t, err)
}

func TestGetEventRankingBorders_EmptyResponse(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(""))
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	_, err := client.GetEventRankingBorders(1)
	assert.Error(t, err)
}

func TestGetEventRankingLogs_BadRequest(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	_, err := client.GetEventRankingLogs(1, models.EventPoint, 100, nil)
	assert.Error(t, err)
}

func TestGetEvents_WithOptions(t *testing.T) {
	expected := []models.Event{
		{Id: 2, Name: "Filtered Event", Type: 3},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Check that query parameters are present
		q := r.URL.Query()
		if q.Get("at") == "" || q.Get("type") == "" || q.Get("orderBy") == "" {
			t.Errorf("Expected query parameters to be set")
		}
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	options := &models.EventsOptions{
		At:       time.Now(),
		Types:    []models.EventType{models.ShowTime, models.Tour},
		OrderBys: []models.EventSortType{models.IdAsc, models.TypeDesc},
	}
	events, err := client.GetEvents(options)
	assert.NoError(t, err)
	assert.Equal(t, expected, events)
}

func TestGetEventRankingLogs_WithOptions(t *testing.T) {
	expected := []models.EventRankingLog{
		{Rank: 2500, Data: []struct {
			Score        int       `json:"score"`
			AggregatedAt time.Time `json:"aggregatedAt"`
		}{
			{Score: 54321, AggregatedAt: time.Now()},
		}},
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("since") == "" {
			t.Errorf("Expected 'since' query parameter to be set")
		}
		if r.Header.Get("If-None-Match") == "" {
			t.Errorf("Expected 'If-None-Match' header to be set")
		}
		json.NewEncoder(w).Encode(expected)
	}
	server, client := setupTestServer(t, handler)
	defer server.Close()

	options := &models.EventRankingLogsOptions{
		Since:      time.Now().Add(-24 * time.Hour),
		IfNonMatch: "etag-value",
	}
	logs, err := client.GetEventRankingLogs(1, models.EventPoint, 2500, options)
	assert.NoError(t, err)
	assert.Equal(t, expected[0].Rank, logs[0].Rank)
	assert.Equal(t, expected[0].Data[0].Score, logs[0].Data[0].Score)
}
