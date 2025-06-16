package dao

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestWriteCSVToR2_AppendMode_FileExists_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	bucket := "b"
	key := "k.csv"
	existing := "id,name\n1,Alice\n"

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Bucket == bucket && *input.Key == key
	})).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(existing)),
	}, nil)

	mockS3.On("PutObject", mock.Anything, mock.MatchedBy(func(input *s3.PutObjectInput) bool {
		bodyBytes, _ := io.ReadAll(input.Body)
		bodyStr := string(bodyBytes)
		expectedSuffix := "2,Bob\n"
		return *input.Bucket == bucket && *input.Key == key && strings.HasPrefix(bodyStr, existing) && strings.HasSuffix(bodyStr, expectedSuffix)
	})).Return(&s3.PutObjectOutput{}, nil)

	type rec struct {
		ID   int    `csv:"id"`
		Name string `csv:"name"`
	}
	records := []rec{{ID: 2, Name: "Bob"}}

	err := writeCSVToR2(mockS3, bucket, key, records, true)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestWriteCSVToR2_AppendMode_FileNotExist_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	bucket := "b"
	key := "k.csv"

	mockS3.On("GetObject", mock.Anything, mock.MatchedBy(func(input *s3.GetObjectInput) bool {
		return *input.Bucket == bucket && *input.Key == key
	})).Return(nil, &types.NoSuchKey{})

	mockS3.On("PutObject", mock.Anything, mock.MatchedBy(func(input *s3.PutObjectInput) bool {
		bodyBytes, _ := io.ReadAll(input.Body)
		bodyStr := string(bodyBytes)
		return *input.Bucket == bucket && *input.Key == key && strings.HasPrefix(bodyStr, "id,name\n") && strings.Contains(bodyStr, "3,Charlie")
	})).Return(&s3.PutObjectOutput{}, nil)

	type rec struct {
		ID   int    `csv:"id"`
		Name string `csv:"name"`
	}
	records := []rec{{ID: 3, Name: "Charlie"}}

	err := writeCSVToR2(mockS3, bucket, key, records, true)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestWriteCSVToR2_AppendMode_GetObjectError(t *testing.T) {
	mockS3 := new(MockS3Client)
	bucket := "b"
	key := "k.csv"

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(nil, errors.New("network error"))

	type rec struct {
		ID   int    `csv:"id"`
		Name string `csv:"name"`
	}
	records := []rec{{ID: 1, Name: "ErrorTest"}}

	err := writeCSVToR2(mockS3, bucket, key, records, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get object")
	mockS3.AssertExpectations(t)
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

	err := writeCSVToR2(mockS3, bucket, key, records, false)
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

	err := writeCSVToR2(mockS3, bucket, key, records, false)
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

func TestSaveEventInfos_FirstTime_SaveSuccess(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(`{"EventId":0}`)),
	}, nil).Once()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Once()

	err := dao.SaveEventInfos([]models.EventInfo{{EventId: 1}})
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveEventInfos_Append_SaveSuccess(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(`{"EventId":10}`)),
	}, nil).Twice()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Once()

	err := dao.SaveEventInfos([]models.EventInfo{{EventId: 11}})
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveEventInfos_GetLatestEventInfoError(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))

	err := dao.SaveEventInfos([]models.EventInfo{{EventId: 1}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail")
	mockS3.AssertExpectations(t)
}

func TestSaveBorderInfos_FirstTime_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(`{"EventId":0}`)),
	}, nil).Once()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Times(2)

	borderInfos := []models.BorderInfo{
		{EventId: 1, Border: 100},
		{EventId: 1, Border: 200},
	}

	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveBorderInfos_Append_SkipOldEvent_Success(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(&s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader(`{"EventId":10}`)),
	}, nil).Once()

	mockS3.On("PutObject", mock.Anything, mock.Anything).Return(&s3.PutObjectOutput{}, nil).Once()

	borderInfos := []models.BorderInfo{
		{EventId: 9, Border: 100},
		{EventId: 10, Border: 200},
	}

	err := dao.SaveBorderInfos(borderInfos)
	assert.NoError(t, err)
	mockS3.AssertExpectations(t)
}

func TestSaveBorderInfos_GetLatestEventInfoError(t *testing.T) {
	mockS3 := new(MockS3Client)
	dao := NewR2DAOWithClient("bucket", "b", "e", "m", mockS3)

	mockS3.On("GetObject", mock.Anything, mock.Anything).Return(nil, errors.New("fail"))

	err := dao.SaveBorderInfos([]models.BorderInfo{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail")
	mockS3.AssertExpectations(t)
}
