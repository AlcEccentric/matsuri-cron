package matsuri

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"time"

	utils "github.com/alceccentric/matsurihi-cron/internal/utils"
	models "github.com/alceccentric/matsurihi-cron/models"

	resty "github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

const (
	BASE_URL_V2 = "https://api.matsurihi.me/api/mltd/v2"
)

// Interface for the Matsurihi.me client to interact with the MLTD API.
type MatsuriClient interface {
	GetEvents(options *models.EventsOptions) ([]models.Event, error)
	GetEvent(eventId int) (models.Event, error)
	GetEventRankingBorders(eventId int) (models.EventRankingBorders, error)
	GetEventRankingLogs(
		eventId int,
		eventType models.EventRankingType,
		rankingBorder int,
		options *models.EventRankingLogsOptions,
	) ([]models.EventRankingLog, error)
}

type MatsurihiMeClient struct {
	baseUrl    string
	httpClient *resty.Client
}

func NewMatsurihiMeClient(baseUrl string) *MatsurihiMeClient {
	httpClient := resty.New()
	httpClient.SetRetryCount(3).
		SetRetryWaitTime(2 * time.Second).
		SetRetryMaxWaitTime(30 * time.Second).
		AddRetryCondition(func(r *resty.Response, err error) bool {
			return r.StatusCode() == 429 || r.StatusCode() == 500 ||
				r.StatusCode() >= 502 && r.StatusCode() <= 504
		})
	return &MatsurihiMeClient{
		baseUrl:    baseUrl,
		httpClient: httpClient,
	}
}

// GetEvents retrieves events based on the provided options:
// - options.At: when specified, it filters events that are active at that time
// - options.Types: the types of events to retrieve
// - options.OrderBys: the order by criteria for the events
// Returns a slice of events or an error if the request fails.
// If options is nil, it retrieves all events without any filters.
func (m *MatsurihiMeClient) GetEvents(options *models.EventsOptions) ([]models.Event, error) {

	url := m.baseUrl + "/events"

	params := make(map[string]string)

	if options != nil {
		if !options.At.IsZero() {
			params["at"] = options.At.Format(time.RFC3339)
		}
		if len(options.Types) > 0 {
			params["type"] = utils.JoinSlice(options.Types, ",")
		}
		if len(options.OrderBys) > 0 {
			params["orderBy"] = utils.JoinSlice(options.OrderBys, ",")
		}
	}

	var events []models.Event

	if err := m.sendGetRequest(url, params, map[string]string{}, &events); err != nil {
		return nil, err
	}

	return events, nil
}

// GetEvent retrieves a specific event by its ID.
// Returns the event or an error if the request fails.
func (m *MatsurihiMeClient) GetEvent(eventId int) (models.Event, error) {

	url := m.baseUrl + "/events/" + strconv.Itoa(eventId)

	var event models.Event

	if err := m.sendGetRequest(url, map[string]string{}, map[string]string{}, &event); err != nil {
		return models.Event{}, err
	}

	return event, nil
}

// GetEventRankingBorders retrieves the ranking borders for a specific event by its ID.
// - eventId: the ID of the event
// Returns ranking borders (i.e., 100, 2500, 5000) available in the event or an error if the request fails.
func (m *MatsurihiMeClient) GetEventRankingBorders(eventId int) (models.EventRankingBorders, error) {

	url := m.baseUrl + "/events/" + strconv.Itoa(eventId) + "/rankings/borders"

	var eventRankingBorders models.EventRankingBorders

	if err := m.sendGetRequest(url, map[string]string{}, map[string]string{}, &eventRankingBorders); err != nil {
		return models.EventRankingBorders{}, err
	}

	return eventRankingBorders, nil
}

// GetEventRankingLogs retrieves the ranking logs for a specific event and type.
// - eventId: the ID of the event
// - eventType: the type of the event ranking (e.g., "eventPoint" or "highScore ")
// - rankingBorder: the border for which to retrieve logs (e.g., 100, 2500, 5000)
// - options: optional parameters for filtering logs
//   - "since": a timestamp to filter logs since that time
//   - "If-None-Match": an ETag value to check for updates
//
// Returns a slice of event ranking logs or an error if the request fails.
// If options is nil, it retrieves all logs without any filters.
func (m *MatsurihiMeClient) GetEventRankingLogs(
	eventId int,
	eventType models.EventRankingType,
	rankingBorder int,
	options *models.EventRankingLogsOptions,
) ([]models.EventRankingLog, error) {

	url := m.baseUrl + "/events/" + strconv.Itoa(eventId) +
		"/rankings/" + string(eventType) +
		"/logs/" + strconv.Itoa(rankingBorder)

	params := make(map[string]string)
	headers := make(map[string]string)

	if options != nil {
		if !options.Since.IsZero() {
			params["since"] = options.Since.Format(time.RFC3339)
		}
		if len(options.IfNonMatch) > 0 {
			headers["If-None-Match"] = options.IfNonMatch
		}
	}

	var eventRankingLogs []models.EventRankingLog

	if err := m.sendGetRequest(url, params, headers, &eventRankingLogs); err != nil {
		return nil, err
	}

	return eventRankingLogs, nil
}

func (m *MatsurihiMeClient) sendGetRequest(
	url string,
	params map[string]string,
	headers map[string]string,
	v interface{},
) error {
	var defaultHeaders = map[string]string{
		"Content-Type": "application/json",
	}

	if headers == nil {
		headers = make(map[string]string)
	}

	maps.Copy(headers, defaultHeaders)
	fullUrl := url + "?" + utils.BuildQueryParams(params)

	logrus.Debug("Sending GET request on url: " + fullUrl +
		" with headers: " + utils.BuildQueryParams(headers) +
		" and params: " + utils.BuildQueryParams(params))

	resp, err := m.httpClient.R().EnableTrace().
		SetHeaders(headers).
		Get(fullUrl)

	if err != nil {
		return err
	}

	if resp.StatusCode() < http.StatusOK || resp.StatusCode() >= http.StatusBadRequest {
		return fmt.Errorf("sending GET request on url %s returned %d", fullUrl, resp.StatusCode())
	}

	if err := json.Unmarshal(resp.Body(), v); err != nil {
		return err
	}

	return nil
}
