package dao

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockS3Client struct {
	mock.Mock
}

func (m *MockS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	args := m.Called(ctx, params)
	resp, _ := args.Get(0).(*s3.GetObjectOutput)
	return resp, args.Error(1)
}

func (m *MockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	args := m.Called(ctx, params)
	resp, _ := args.Get(0).(*s3.PutObjectOutput)
	return resp, args.Error(1)
}

func TestWriteCSVToR2_Overwrite_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	bucket := "b"
	key := "k.csv"

	mockS3.On("PutObject", mock.Anything, mock.MatchedBy(func(input *s3.PutObjectInput) bool {
		bodyBytes, _ := io.ReadAll(input.Body)
		bodyStr := string(bodyBytes)
		return *input.Bucket == bucket && *input.Key == key && strings.HasPrefix(bodyStr, "id,name\n") && strings.Contains(bodyStr, "4,Dana")
	})).Return(&s3.PutObjectOutput{}, nil)

	type rec struct {
		ID   int    `csv:"id"`
		Name string `csv:"name"`
	}
	records := []rec{{ID: 4, Name: "Dana"}}

	err := writeCSVToR2(mockS3, bucket, key, records)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestWriteCSVToR2_PutObjectError(t *testing.T) {
	mockS3 := new(MockS3Client)
	bucket := "b"
	key := "k.csv"

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(nil, errors.New("put failed"))

	type rec struct {
		ID   int    `csv:"id"`
		Name string `csv:"name"`
	}
	records := []rec{{ID: 5, Name: "FailPut"}}

	err := writeCSVToR2(mockS3, bucket, key, records)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "put failed")
	mockS3.AssertExpectations(t)
}

func TestGetLatestEventInfo_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	jsonStr := `{"EventId":10}`
	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Bucket == "bucket" && strings.Contains(*input.Key, LATEST_EVENT_BORDER_INFO_FILE)
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(jsonStr)),
	}, nil)

	eventInfo, err := dao.GetLatestEventInfo()
	assert.NoError(t, err)
	assert.Equal(t, 10, eventInfo.EventId)
	mockS3.AssertExpectations(t)
}

func TestGetLatestEventInfo_GetObjectError(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(nil, errors.New("get failed"))

	_, err := dao.GetLatestEventInfo()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get failed")
	mockS3.AssertExpectations(t)
}

func TestGetLatestEventInfo_JSONDecodeError(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader("not json")),
	}, nil)

	_, err := dao.GetLatestEventInfo()
	assert.Error(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveEventInfos_SaveSuccess(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Once()

	err := dao.SaveEventInfos([]models.EventInfo{{EventId: 1}})
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveLatestEventInfo_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	info := models.EventInfo{
		EventId: 99,
	}

	mockS3.On("PutObject", mock.Anything, mock.MatchedBy(func(input *s3.PutObjectInput) bool {
		return *input.Bucket == "bucket" &&
			strings.HasSuffix(*input.Key, LATEST_EVENT_BORDER_INFO_FILE)
	})).Return(&s3.PutObjectOutput{}, nil).Once()

	err := dao.SaveLatestEventInfo(info)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveLatestEventInfo_PutObjectError(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	info := models.EventInfo{
		EventId: 100,
	}

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(nil, errors.New("put error")).Once()

	err := dao.SaveLatestEventInfo(info)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "put error")
	mockS3.AssertExpectations(t)
}

func TestSaveBorderInfos_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)
	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Times(2)

	borderInfos := []models.BorderInfo{
		{EventId: 1, IdolId: 0, Border: 100},
		{EventId: 1, IdolId: 0, Border: 200},
	}

	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}
