package jobs

import (
	"errors"
	"testing"
	"time"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// --- Mocks ---

type MockDAO struct {
	mock.Mock
}

func (m *MockDAO) SaveEventInfos(eventInfos []models.EventInfo) error {
	args := m.Called(eventInfos)
	return args.Error(0)
}
func (m *MockDAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	args := m.Called(borderInfos)
	return args.Error(0)
}
func (m *MockDAO) GetLatestEventInfo() (models.EventInfo, error) {
	args := m.Called()
	return args.Get(0).(models.EventInfo), args.Error(1)
}
func (m *MockDAO) SaveLatestEventInfo(info models.EventInfo) error {
	args := m.Called(info)
	return args.Error(0)
}

type MockMatsuriClient struct {
	mock.Mock
}

func (m *MockMatsuriClient) GetEvents(options *models.EventsOptions) ([]models.Event, error) {
	args := m.Called(options)
	return args.Get(0).([]models.Event), args.Error(1)
}
func (m *MockMatsuriClient) GetEvent(eventId int) (models.Event, error) {
	args := m.Called(eventId)
	return args.Get(0).(models.Event), args.Error(1)
}
func (m *MockMatsuriClient) GetEventRankingBorders(eventId int) (models.EventRankingBorders, error) {
	args := m.Called(eventId)
	return args.Get(0).(models.EventRankingBorders), args.Error(1)
}
func (m *MockMatsuriClient) GetEventRankingLogs(eventId int, eventType models.EventRankingType, rankingBorder int, options *models.EventRankingLogsOptions) ([]models.EventRankingLog, error) {
	args := m.Called(eventId, eventType, rankingBorder, options)
	return args.Get(0).([]models.EventRankingLog), args.Error(1)
}

func (m *MockMatsuriClient) GetEventIdolRankingLogs(eventId int, rankingBorder int, options *models.EventRankingLogsOptions) (map[int][]models.EventRankingLog, error) {
	args := m.Called(eventId, rankingBorder, options)
	return args.Get(0).(map[int][]models.EventRankingLog), args.Error(1)
}

// --- Tests ---

func TestRunSync_HappyPath(t *testing.T) {
	mockDao := new(MockDAO)
	mockClient := new(MockMatsuriClient)

	latest := models.EventInfo{EventId: 1}
	events := []models.Event{
		{Id: 2, Type: int(models.Theater), Name: "Event2", Schedule: struct {
			BeginAt      time.Time "json:\"beginAt\""
			EndAt        time.Time "json:\"endAt\""
			PageOpenedAt time.Time "json:\"pageOpenedAt\""
			PageClosedAt time.Time "json:\"pageClosedAt\""
			BoostBeginAt time.Time "json:\"boostBeginAt\""
			BoostEndAt   time.Time "json:\"boostEndAt\""
		}{
			BeginAt: time.Now(), EndAt: time.Now().Add(24 * time.Hour),
		}},
	}
	mockDao.On("GetLatestEventInfo").Return(latest, nil).Once()
	mockClient.On("GetEvents", mock.Anything).Return(events, nil).Once()
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{EventPoint: []int{100, 2500}}, nil).Once()
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockDao.On("SaveEventInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveBorderInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveLatestEventInfo", mock.Anything).Return(nil).Once()

	err := RunSync(mockClient, mockDao)
	assert.NoError(t, err)
	mockDao.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRunSync_GetLatestEventInfoError(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{}, errors.New("fail")).Once()
	mockClient := new(MockMatsuriClient)

	err := RunSync(mockClient, mockDao)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get latest event info")
}

func TestRunSync_GetEventsError(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{EventId: 1}, nil).Once()
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEvents", mock.Anything).Return([]models.Event{}, errors.New("fail")).Once()

	err := RunSync(mockClient, mockDao)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get events")
}

func TestRunSync_NoNewEvents(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{EventId: 1}, nil).Once()
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEvents", mock.Anything).Return([]models.Event{}, nil).Once()

	// Border info should still be collected for latest event id (1)
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil).Once()
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil).Once()

	// SaveBorderInfos and SaveLatestEventInfo should be called
	mockDao.On("SaveBorderInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveLatestEventInfo", mock.Anything).Return(nil).Once()

	// SaveEventInfos should NOT be called, but if you want to enforce this:
	// mockDao.AssertNotCalled(t, "SaveEventInfos", mock.Anything)

	err := RunSync(mockClient, mockDao)
	assert.NoError(t, err)
	mockDao.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestRunSync_SaveEventInfosError(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{EventId: 1}, nil).Once()
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEvents", mock.Anything).Return([]models.Event{
		{Id: 2, Type: int(models.Theater), Name: "Event2", Schedule: struct {
			BeginAt      time.Time "json:\"beginAt\""
			EndAt        time.Time "json:\"endAt\""
			PageOpenedAt time.Time "json:\"pageOpenedAt\""
			PageClosedAt time.Time "json:\"pageClosedAt\""
			BoostBeginAt time.Time "json:\"boostBeginAt\""
			BoostEndAt   time.Time "json:\"boostEndAt\""
		}{
			BeginAt: time.Now(), EndAt: time.Now().Add(24 * time.Hour),
		}},
	}, nil).Once()
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{EventPoint: []int{100, 2500}}, nil).Once()
	mockDao.On("SaveEventInfos", mock.Anything).Return(errors.New("fail")).Once()

	err := RunSync(mockClient, mockDao)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "save event infos")
}

func TestRunSync_SaveBorderInfosError(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{EventId: 1}, nil).Once()
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEvents", mock.Anything).Return([]models.Event{
		{Id: 2, Type: int(models.Theater), Name: "Event2", Schedule: struct {
			BeginAt      time.Time "json:\"beginAt\""
			EndAt        time.Time "json:\"endAt\""
			PageOpenedAt time.Time "json:\"pageOpenedAt\""
			PageClosedAt time.Time "json:\"pageClosedAt\""
			BoostBeginAt time.Time "json:\"boostBeginAt\""
			BoostEndAt   time.Time "json:\"boostEndAt\""
		}{
			BeginAt: time.Now(), EndAt: time.Now().Add(24 * time.Hour),
		}},
	}, nil).Once()
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{EventPoint: []int{100, 2500}}, nil).Once()
	// Add these lines:
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	// End added lines
	mockDao.On("SaveEventInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveBorderInfos", mock.Anything).Return(errors.New("fail")).Once()

	err := RunSync(mockClient, mockDao)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "save border infos")
}

func TestRunSync_SaveLatestEventInfoError(t *testing.T) {
	mockDao := new(MockDAO)
	mockDao.On("GetLatestEventInfo").Return(models.EventInfo{EventId: 1}, nil).Once()
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEvents", mock.Anything).Return([]models.Event{
		{Id: 2, Type: int(models.Theater), Name: "Event2", Schedule: struct {
			BeginAt      time.Time "json:\"beginAt\""
			EndAt        time.Time "json:\"endAt\""
			PageOpenedAt time.Time "json:\"pageOpenedAt\""
			PageClosedAt time.Time "json:\"pageClosedAt\""
			BoostBeginAt time.Time "json:\"boostBeginAt\""
			BoostEndAt   time.Time "json:\"boostEndAt\""
		}{
			BeginAt: time.Now(), EndAt: time.Now().Add(24 * time.Hour),
		}},
	}, nil).Once()
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{EventPoint: []int{100, 2500}}, nil).Once()
	// Add these lines:
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	mockClient.On("GetEventRankingLogs", 2, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil)
	// End added lines
	mockDao.On("SaveEventInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveBorderInfos", mock.Anything).Return(nil).Once()
	mockDao.On("SaveLatestEventInfo", mock.Anything).Return(errors.New("fail")).Once()

	err := RunSync(mockClient, mockDao)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "save latest event info")
}

// --- Helper function tests ---

func TestCollectEventInfos_SkipOldAndAnniversary(t *testing.T) {
	mockClient := new(MockMatsuriClient)
	events := []models.Event{
		{Id: 1, Type: int(models.Anniversary), Name: "Anniv"},
		{Id: 2, Type: int(models.Theater), Name: "Theater"},
	}
	// Make event 2 fail as well, so both are skipped
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{}, errors.New("fail")).Once()
	infos := collectEventInfos(mockClient, events, 1)
	assert.Len(t, infos, 0)
}

func TestCollectEventInfos_HandlesGetEventRankingBordersError(t *testing.T) {
	mockClient := new(MockMatsuriClient)
	events := []models.Event{
		{Id: 2, Type: int(models.Theater), Name: "Theater"},
	}
	mockClient.On("GetEventRankingBorders", 2).Return(models.EventRankingBorders{}, errors.New("fail")).Once()
	infos := collectEventInfos(mockClient, events, 1)
	assert.Len(t, infos, 0)
}

func TestCollectBorderInfos_HandlesGetEventRankingLogsError(t *testing.T) {
	mockClient := new(MockMatsuriClient)
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 100, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, errors.New("fail")).Once()
	mockClient.On("GetEventRankingLogs", 1, models.EventPoint, 2500, (*models.EventRankingLogsOptions)(nil)).Return([]models.EventRankingLog{}, nil).Once()
	infos := collectBorderInfos(mockClient, map[int]struct{}{1: struct{}{}}, map[int]models.EventInfo{1: models.EventInfo{EventId: 1}})
	assert.Len(t, infos, 0)
}

func TestCollectAnniversaryBorders_Success(t *testing.T) {
	mockClient := new(MockMatsuriClient)
	eventId := 10
	border := 100
	now := time.Now()
	mockClient.On("GetEventIdolRankingLogs", eventId, border, (*models.EventRankingLogsOptions)(nil)).
		Return(map[int][]models.EventRankingLog{
			1: {
				{
					Rank: 100,
					Data: []struct {
						Score        int       `json:"score"`
						AggregatedAt time.Time `json:"aggregatedAt"`
					}{
						{Score: 123, AggregatedAt: now},
					},
				},
			},
		}, nil).Once()
	// For the second border (1000), return empty
	mockClient.On("GetEventIdolRankingLogs", eventId, 1000, (*models.EventRankingLogsOptions)(nil)).
		Return(map[int][]models.EventRankingLog{}, nil).Once()

	infos := collectAnniversaryBorders(mockClient, eventId)
	assert.Len(t, infos, 1)
	assert.Equal(t, eventId, infos[0].EventId)
	assert.Equal(t, border, infos[0].Border)
	assert.Equal(t, 1, infos[0].IdolId)
	assert.Equal(t, 123, infos[0].Score)
	assert.Equal(t, now, infos[0].AggregatedAt)
}

func TestCollectAnniversaryBorders_Error(t *testing.T) {
	mockClient := new(MockMatsuriClient)
	eventId := 10
	// Simulate error for both borders
	mockClient.On("GetEventIdolRankingLogs", eventId, 100, (*models.EventRankingLogsOptions)(nil)).
		Return(map[int][]models.EventRankingLog{}, errors.New("fail")).Once()
	mockClient.On("GetEventIdolRankingLogs", eventId, 1000, (*models.EventRankingLogsOptions)(nil)).
		Return(map[int][]models.EventRankingLog{}, errors.New("fail")).Once()

	infos := collectAnniversaryBorders(mockClient, eventId)
	assert.Len(t, infos, 0)
}

func TestIsSupportedAnniversaryEvent_True(t *testing.T) {
	event := models.Event{
		Id:   1,
		Type: int(models.Anniversary),
	}
	// 52 idol points, each with both supported borders
	borders := models.EventRankingBorders{
		IdolPoint: make([]models.IdolPointBorders, 52),
	}
	for i := 0; i < 52; i++ {
		borders.IdolPoint[i] = models.IdolPointBorders{
			IdolId:  i + 1,
			Borders: []int{100, 1000},
		}
	}
	assert.True(t, isSupportedAnniversaryEvent(event, borders, []int{100, 1000}))
}

func TestIsSupportedAnniversaryEvent_WrongType(t *testing.T) {
	event := models.Event{
		Id:   1,
		Type: int(models.Theater),
	}
	borders := models.EventRankingBorders{}
	assert.False(t, isSupportedAnniversaryEvent(event, borders, []int{100, 1000}))
}

func TestIsSupportedAnniversaryEvent_WrongIdolCount(t *testing.T) {
	event := models.Event{
		Id:   1,
		Type: int(models.Anniversary),
	}
	borders := models.EventRankingBorders{
		IdolPoint: make([]models.IdolPointBorders, 51), // not 52
	}
	result := isSupportedAnniversaryEvent(event, borders, []int{100, 1000})
	assert.False(t, result)
}

func TestIsSupportedAnniversaryEvent_WrongSupportedCount(t *testing.T) {
	event := models.Event{
		Id:   1,
		Type: int(models.Anniversary),
	}
	borders := models.EventRankingBorders{
		IdolPoint: make([]models.IdolPointBorders, 52),
	}
	// Only 51 idols have the supported borders
	for i := 0; i < 51; i++ {
		borders.IdolPoint[i] = models.IdolPointBorders{
			IdolId:  i + 1,
			Borders: []int{100, 1000},
		}
	}
	// The last idol does not have all supported borders
	borders.IdolPoint[51] = models.IdolPointBorders{
		IdolId:  52,
		Borders: []int{100},
	}
	result := isSupportedAnniversaryEvent(event, borders, []int{100, 1000})
	assert.False(t, result)
}
