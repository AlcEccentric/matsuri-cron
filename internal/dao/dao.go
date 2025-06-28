package dao

import (
	"context"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	LATEST_EVENT_BORDER_INFO_FILE = "latest_event_border_info.json"
	EVENT_INFO_FILENAME           = "event_info_all.csv"
	BORDER_INFO_FILENAME_FORMAT   = "border_info_%d_%d_%d.csv"
)

type BorderGroupKey struct {
	EventId int
	IdolId  int
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
	SaveLatestEventInfo(models.EventInfo) error
}

func groupByEventIdAndBorder(infos []models.BorderInfo) map[BorderGroupKey][]models.BorderInfo {
	groups := make(map[BorderGroupKey][]models.BorderInfo)
	for _, info := range infos {
		key := BorderGroupKey{EventId: info.EventId, Border: info.Border, IdolId: info.IdolId}
		groups[key] = append(groups[key], info)
	}
	return groups
}
