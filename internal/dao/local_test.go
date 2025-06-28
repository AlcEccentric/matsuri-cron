package dao

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/stretchr/testify/assert"
)

func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "localdao_test")
	assert.NoError(t, err)
	return dir
}

func writeJSONFile(t *testing.T, path string, v interface{}) {
	f, err := os.Create(path)
	assert.NoError(t, err)
	defer f.Close()
	enc := json.NewEncoder(f)
	assert.NoError(t, enc.Encode(v))
}

func TestGetLatestEventInfo_FileNotExist(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	info, err := dao.GetLatestEventInfo()
	assert.NoError(t, err)
	assert.Equal(t, 0, info.EventId)
}

func TestGetLatestEventInfo_FileExists_Valid(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	expected := models.EventInfo{
		EventId: 123,
		StartAt: time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
	}
	jsonPath := filepath.Join(tmp, metadataDir, "latest_event_border_info.json")
	writeJSONFile(t, jsonPath, expected)
	dao := NewLocalDAO(tmp, "b", "e", metadataDir)
	info, err := dao.GetLatestEventInfo()
	assert.NoError(t, err)
	assert.Equal(t, expected.EventId, info.EventId)
	assert.Equal(t, time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC), info.StartAt)
}

func TestGetLatestEventInfo_FileExists_Invalid(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	jsonPath := filepath.Join(tmp, metadataDir, "latest_event_border_info.json")
	os.WriteFile(jsonPath, []byte("not json"), 0644)
	dao := NewLocalDAO(tmp, "b", "e", metadataDir)
	_, err := dao.GetLatestEventInfo()
	assert.Error(t, err)
}

func TestSaveEventInfos(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	eventInfos := []models.EventInfo{
		{EventId: 1},
		{EventId: 2},
	}
	err := dao.SaveEventInfos(eventInfos)
	assert.NoError(t, err)
	// Check file exists in the correct directory
	_, err = os.Stat(filepath.Join(tmp, "e", EVENT_INFO_FILENAME))
	assert.True(t, os.IsNotExist(err) == false)
}

func TestSaveBorderInfos(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	borderInfos := []models.BorderInfo{
		{EventId: 1, IdolId: 0, Border: 100, Score: 10, AggregatedAt: time.Now()},
		{EventId: 1, IdolId: 0, Border: 100, Score: 20, AggregatedAt: time.Now()},
		{EventId: 2, IdolId: 0, Border: 2500, Score: 30, AggregatedAt: time.Now()},
	}
	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
	// Check files exist
	_, err = os.Stat(filepath.Join(tmp, "b", "border_info_1_0_100.csv"))
	assert.True(t, os.IsNotExist(err) == false)
	_, err = os.Stat(filepath.Join(tmp, "b", "border_info_2_0_2500.csv"))
	assert.True(t, os.IsNotExist(err) == false)
}

func TestSaveEventInfos_Empty(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	err := dao.SaveEventInfos([]models.EventInfo{})
	assert.NoError(t, err)
}

func TestSaveLatestEventInfo(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	info := models.EventInfo{
		EventId: 42,
		StartAt: time.Date(2025, 6, 16, 12, 0, 0, 0, time.UTC),
	}
	err := dao.SaveLatestEventInfo(info)
	assert.NoError(t, err)

	// Check file exists and content is correct
	filePath := filepath.Join(tmp, "m", LATEST_EVENT_BORDER_INFO_FILE)
	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)

	var got models.EventInfo
	err = json.Unmarshal(data, &got)
	assert.NoError(t, err)
	assert.Equal(t, info.EventId, got.EventId)
	assert.Equal(t, info.StartAt, got.StartAt)
}

func TestSaveBorderInfos_Empty(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	err := dao.SaveBorderInfos([]models.BorderInfo{})
	assert.NoError(t, err)
}
