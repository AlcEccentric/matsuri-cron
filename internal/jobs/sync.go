package jobs

import (
	"errors"
	"strconv"

	"github.com/alceccentric/matsurihi-cron/internal/dao"
	"github.com/alceccentric/matsurihi-cron/internal/matsuri"
	utils "github.com/alceccentric/matsurihi-cron/internal/utils"
	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/sirupsen/logrus"
)

var SURPPORTED_BORDERS = []int{100, 2500}
var ANN_SUPPORTED_BORDERS = []int{100, 1000}

const (
	SURPPORTED_BORDER_TYPE = models.EventPoint
)

func RunSync(client matsuri.MatsuriClient, dao dao.DAO) error {
	latest, err := dao.GetLatestEventInfo()
	if err != nil {
		return errors.New("get latest event info: " + err.Error())
	}

	events, err := client.GetEvents(&models.EventsOptions{
		OrderBys: []models.EventSortType{models.IdAsc},
		Types:    []models.EventType{models.Theater, models.Tour, models.Tune, models.Tale, models.Anniversary},
	})
	if err != nil {
		return errors.New("get events: " + err.Error())
	}
	logrus.Infof("Got %d events before filtering", len(events))

	eventInfos := collectEventInfos(client, events, latest.EventId)
	if len(eventInfos) > 0 {
		logrus.Infof("Got %d events to process", len(eventInfos))
		if err := dao.SaveEventInfos(eventInfos); err != nil {
			return errors.New("save event infos: " + err.Error())
		}
	}

	eventIdToEventInfo := make(map[int]models.EventInfo)
	for _, info := range eventInfos {
		eventIdToEventInfo[info.EventId] = info
	}

	eventIds := make(map[int]struct{})
	if latest.EventId > 0 {
		// Although eventInfos only contains events with IDs greater than latest.EventId,
		// we still wanna fetch border infos for the last latest event
		eventIds[latest.EventId] = struct{}{}
	}
	for _, info := range eventInfos {
		eventIds[info.EventId] = struct{}{}
		if info.EventId > latest.EventId {
			latest = info
		}
	}

	borderInfos := collectBorderInfos(client, eventIds, eventIdToEventInfo)
	if err := dao.SaveBorderInfos(borderInfos); err != nil {
		return errors.New("save border infos: " + err.Error())
	}
	// TODO: Define a new struct for latest event info to include name but not type
	if err := dao.SaveLatestEventInfo(latest); err != nil {
		return errors.New("save latest event info: " + err.Error())
	}
	logrus.Info("Job completed successfully.")
	return nil
}

func collectBorderInfos(
	matsuriClient matsuri.MatsuriClient,
	eventIds map[int]struct{},
	eventIdToEventInfo map[int]models.EventInfo,
) []models.BorderInfo {
	var borderInfos []models.BorderInfo

	for eventId := range eventIds {
		eventInfo := eventIdToEventInfo[eventId]
		if eventInfo.EventType == models.Anniversary {
			borderInfos = append(borderInfos, collectAnniversaryBorders(matsuriClient, eventId)...)
		} else {
			borderInfos = append(borderInfos, collectNormalBorders(matsuriClient, eventId)...)
		}
	}
	return borderInfos
}

func collectAnniversaryBorders(client matsuri.MatsuriClient, eventId int) []models.BorderInfo {
	var infos []models.BorderInfo
	for _, border := range ANN_SUPPORTED_BORDERS {
		logrus.Infof("Collecting border infos for anniversary event %d with border: %d", eventId, border)
		idolRankingLogs, err := client.GetEventIdolRankingLogs(eventId, border, nil)
		if err != nil {
			logrus.Warnf("Failed to get ranking logs for event %d with border: %d : %s", eventId, border, err.Error())
			continue
		}
		logCnt := 0
		for idolId, rankingLogs := range idolRankingLogs {
			for _, log := range rankingLogs {
				logCnt += len(log.Data)
				for _, data := range log.Data {
					infos = append(infos, models.BorderInfo{
						EventId:      eventId,
						Border:       border,
						IdolId:       idolId,
						RankingType:  models.IdolPoint,
						Score:        data.Score,
						AggregatedAt: data.AggregatedAt,
					})
				}
			}
		}
		logrus.Infof("Collected %d border infos for event %d with border: %d", logCnt, eventId, border)
	}
	return infos
}

func collectNormalBorders(client matsuri.MatsuriClient, eventId int) []models.BorderInfo {
	var infos []models.BorderInfo
	for _, border := range SURPPORTED_BORDERS {
		logrus.Infof("Collecting border infos for normal event %d with border: %d", eventId, border)
		rankingLogs, err := client.GetEventRankingLogs(eventId, SURPPORTED_BORDER_TYPE, border, nil)
		if err != nil {
			logrus.Warnf("Failed to get ranking logs for event %d with border: %d : %s", eventId, border, err.Error())
			continue
		}
		logCnt := 0
		for _, log := range rankingLogs {
			logCnt += len(log.Data)
			for _, data := range log.Data {
				infos = append(infos, models.BorderInfo{
					EventId:      eventId,
					Border:       border,
					RankingType:  SURPPORTED_BORDER_TYPE,
					Score:        data.Score,
					AggregatedAt: data.AggregatedAt,
				})
			}
		}
		logrus.Infof("Collected %d border infos for event %d with border: %d", logCnt, eventId, border)
	}
	return infos
}

func collectEventInfos(
	matsuriClient matsuri.MatsuriClient,
	events []models.Event,
	maxEventId int,
) []models.EventInfo {
	eventInfos := make([]models.EventInfo, 0)

	for _, event := range events {
		if event.Id <= maxEventId {
			continue
		}

		borders, err := matsuriClient.GetEventRankingBorders(event.Id)
		if err != nil {
			logrus.Warn("Failed to get borders for event " + strconv.Itoa(event.Id) + ": " + err.Error())
			continue
		}

		if isSupportedAnniversaryEvent(event, borders, ANN_SUPPORTED_BORDERS) ||
			isSupportedNormalEvent(event, borders, SURPPORTED_BORDERS) {
			eventInfo := models.EventInfo{
				EventId:           event.Id,
				EventType:         models.EventType(event.Type),
				EventName:         event.Name,
				InternalEventType: models.ToInternalEventType(event),
				StartAt:           event.Schedule.BeginAt,
				EndAt:             event.Schedule.EndAt,
				BoostAt:           event.Schedule.BoostBeginAt,
			}
			logrus.Infof("Collected info for event %d", event.Id)
			eventInfos = append(eventInfos, eventInfo)
		} else {
			logrus.Infof("Event %d with type %d is not supported", event.Id, event.Type)
		}
	}

	return eventInfos
}

func isSupportedNormalEvent(event models.Event, borders models.EventRankingBorders, supportedBorders []int) bool {
	return models.EventType(event.Type) != models.Anniversary && utils.IsSubset(supportedBorders, borders.EventPoint)
}

func isSupportedAnniversaryEvent(event models.Event, borders models.EventRankingBorders, anniversarySupportedBorders []int) bool {
	if models.EventType(event.Type) != models.Anniversary {
		return false
	}

	if len(borders.IdolPoint) != 52 {
		logrus.Fatalf("Event %d has %d idol points, expected 52", event.Id, len(borders.IdolPoint))
	}

	counter := 0
	for _, idolPoint := range borders.IdolPoint {
		if utils.IsSubset(anniversarySupportedBorders, idolPoint.Borders) {
			counter++
		}
	}
	if counter != 52 {
		logrus.Fatalf("Event %d has %d idols with supported anniversary borders, expected 52", event.Id, counter)
	}

	return true
}
