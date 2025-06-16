package dao

import (
	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/sirupsen/logrus"
)

const (
	EventInfoFileName    = "event_info_all.csv"
	BorderInfoFileFormat = "border_info_%d_%d.csv"
)

type BorderGroupKey struct {
	EventId int
	Border  int
}

type DAO interface {
	SaveEventInfos(eventInfos []models.EventInfo) error
	SaveBorderInfos(borderInfos []models.BorderInfo) error
	GetMetadataInfo() (models.LatestEventBorderInfo, error)
}

func groupByEventIdAndBorder(infos []models.BorderInfo) map[BorderGroupKey][]models.BorderInfo {
	groups := make(map[BorderGroupKey][]models.BorderInfo)
	for _, info := range infos {
		key := BorderGroupKey{EventId: info.EventId, Border: info.Border}
		groups[key] = append(groups[key], info)
	}
	return groups
}

func filterEventInfos(infos []models.EventInfo, latestInfo models.LatestEventBorderInfo) []models.EventInfo {
	var filteredInfos []models.EventInfo
	var filteredEventIds []int
	logrus.Infof("Filtering %d event infos with latest event ID: %d", len(infos), latestInfo.EventId)
	for _, info := range infos {
		if info.EventId > latestInfo.EventId {
			filteredInfos = append(filteredInfos, info)
			filteredEventIds = append(filteredEventIds, info.EventId)
		}
	}
	logrus.Infof("Filtered %d event infos with new event IDs: %v", len(filteredInfos), filteredEventIds)
	return filteredInfos
}

func filterBorderInfos(infos []models.BorderInfo, latestInfo models.LatestEventBorderInfo) []models.BorderInfo {
	var filteredInfos []models.BorderInfo
	logrus.Infof("Filtering %d border infos with latest event ID: %d, latest aggregated at info: %v", len(infos), latestInfo.EventId,
		latestInfo.LastAggregatedAtByBorder)
	for _, info := range infos {
		if info.EventId > latestInfo.EventId ||
			(info.EventId == latestInfo.EventId && info.AggregatedAt.After(latestInfo.LastAggregatedAtByBorder[info.Border])) {
			filteredInfos = append(filteredInfos, info)
		}
	}
	logrus.Infof("Filtered %d border infos", len(filteredInfos))
	return filteredInfos
}
