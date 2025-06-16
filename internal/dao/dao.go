package dao

import (
	"context"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

const (
	LATEST_EVENT_BORDER_INFO_FILE = "latest_event_border_info.json"
	EVENT_INFO_FILENAME           = "event_info_all.csv"
	BORDER_INFO_FILENAME_FORMAT   = "border_info_%d_%d.csv"
)

type BorderGroupKey struct {
	EventId int
	Border  int
}

type S3Uploader interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type DAO interface {
	SaveEventInfos(eventInfos []models.EventInfo) error
	SaveBorderInfos(borderInfos []models.BorderInfo) error
	GetLatestEventInfo() (models.EventInfo, error)
}

func groupByEventIdAndBorder(infos []models.BorderInfo) map[BorderGroupKey][]models.BorderInfo {
	groups := make(map[BorderGroupKey][]models.BorderInfo)
	for _, info := range infos {
		key := BorderGroupKey{EventId: info.EventId, Border: info.Border}
		groups[key] = append(groups[key], info)
	}
	return groups
}

func filterEventInfos(infos []models.EventInfo, latestInfo models.EventInfo) []models.EventInfo {
	var filteredInfos []models.EventInfo
	var filteredEventIds []int
	logrus.Infof("Filtering %d event infos with latest event ID: %d", len(infos), latestInfo.EventId)
	for _, info := range infos {
		if info.EventId > latestInfo.EventId {
			filteredInfos = append(filteredInfos, info)
			filteredEventIds = append(filteredEventIds, info.EventId)
		}
	}
	logrus.Infof("Retained %d event infos with new event IDs: %v", len(filteredInfos), filteredEventIds)
	return filteredInfos
}
