package dao

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/stretchr/testify/assert"
)

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "localdao_test")
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

func TestGetMetadataInfo_FileNotExist(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	info, err := dao.GetMetadataInfo()
	assert.NoError(t, err)
	assert.Equal(t, 0, info.EventId)
	assert.Empty(t, info.LastAggregatedAtByBorder)
}

func TestGetMetadataInfo_FileExists_Valid(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	expected := models.LatestEventBorderInfo{
		EventId: 123,
		LastAggregatedAtByBorder: map[int]time.Time{
			100: time.Now(),
		},
	}
	jsonPath := filepath.Join(tmp, metadataDir, "latest_event_border_info.json")
	writeJSONFile(t, jsonPath, expected)
	dao := NewLocalDAO(tmp, "b", "e", metadataDir)
	info, err := dao.GetMetadataInfo()
	assert.NoError(t, err)
	assert.Equal(t, expected.EventId, info.EventId)
	assert.Equal(t, expected.LastAggregatedAtByBorder[100].Unix(), info.LastAggregatedAtByBorder[100].Unix())
}

func TestGetMetadataInfo_FileExists_Invalid(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	jsonPath := filepath.Join(tmp, metadataDir, "latest_event_border_info.json")
	os.WriteFile(jsonPath, []byte("not json"), 0644)
	dao := NewLocalDAO(tmp, "b", "e", metadataDir)
	_, err := dao.GetMetadataInfo()
	assert.Error(t, err)
}

func TestSaveEventInfos_FirstTime(t *testing.T) {
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
	_, err = os.Stat(filepath.Join(tmp, "e", EventInfoFileName))
	assert.True(t, os.IsNotExist(err) == false)
}

func TestSaveEventInfos_Filtered(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	eventDir := "e"
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	latest := models.LatestEventBorderInfo{EventId: 1}
	writeJSONFile(t, filepath.Join(tmp, metadataDir, "latest_event_border_info.json"), latest)
	dao := NewLocalDAO(tmp, "b", eventDir, metadataDir)
	eventInfos := []models.EventInfo{
		{EventId: 1},
		{EventId: 2},
	}
	err := dao.SaveEventInfos(eventInfos)
	assert.NoError(t, err)
}

func TestSaveEventInfos_ErrorOnMetadata(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	eventDir := "e"
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	// Write invalid JSON
	os.WriteFile(filepath.Join(tmp, metadataDir, "latest_event_border_info.json"), []byte("bad"), 0644)
	dao := NewLocalDAO(tmp, "b", eventDir, metadataDir)
	eventInfos := []models.EventInfo{{EventId: 1}}
	err := dao.SaveEventInfos(eventInfos)
	assert.Error(t, err)
}

func TestSaveBorderInfos_FirstTime(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	borderInfos := []models.BorderInfo{
		{EventId: 1, Border: 100, Score: 10, AggregatedAt: time.Now()},
		{EventId: 1, Border: 100, Score: 20, AggregatedAt: time.Now()},
		{EventId: 2, Border: 2500, Score: 30, AggregatedAt: time.Now()},
	}
	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
	// Check files exist
	_, err = os.Stat(filepath.Join(tmp, "b", "border_info_1_100.csv"))
	assert.True(t, os.IsNotExist(err) == false)
	_, err = os.Stat(filepath.Join(tmp, "b", "border_info_2_2500.csv"))
	assert.True(t, os.IsNotExist(err) == false)
}

func TestSaveBorderInfos_Filtered(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	borderDir := "b"
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	latest := models.LatestEventBorderInfo{
		EventId: 1,
		LastAggregatedAtByBorder: map[int]time.Time{
			100: time.Now().Add(-time.Hour),
		},
	}
	writeJSONFile(t, filepath.Join(tmp, metadataDir, "latest_event_border_info.json"), latest)
	dao := NewLocalDAO(tmp, borderDir, "e", metadataDir)
	borderInfos := []models.BorderInfo{
		{EventId: 1, Border: 100, Score: 10, AggregatedAt: time.Now()},
		{EventId: 2, Border: 2500, Score: 30, AggregatedAt: time.Now()},
	}
	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
}

func TestSaveBorderInfos_ErrorOnMetadata(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	borderDir := "b"
	metadataDir := "m"
	os.MkdirAll(filepath.Join(tmp, metadataDir), 0755)
	os.WriteFile(filepath.Join(tmp, metadataDir, "latest_event_border_info.json"), []byte("bad"), 0644)
	dao := NewLocalDAO(tmp, borderDir, "e", metadataDir)
	borderInfos := []models.BorderInfo{
		{EventId: 1, Border: 100, Score: 10, AggregatedAt: time.Now()},
	}
	err := dao.SaveBorderInfos(borderInfos)
	assert.Error(t, err)
}

func TestSaveEventInfos_Empty(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	err := dao.SaveEventInfos([]models.EventInfo{})
	assert.NoError(t, err)
}

func TestSaveBorderInfos_Empty(t *testing.T) {
	tmp := createTempDir(t)
	defer os.RemoveAll(tmp)
	dao := NewLocalDAO(tmp, "b", "e", "m")
	err := dao.SaveBorderInfos([]models.BorderInfo{})
	assert.NoError(t, err)
}
