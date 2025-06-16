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
		Types:    []models.EventType{models.Theater, models.Tour, models.Tune, models.Tale},
	})
	if err != nil {
		return errors.New("get events: " + err.Error())
	}
	logrus.Infof("Got %d events before filtering", len(events))

	eventInfos := collectEventInfos(client, events, latest.EventId, SURPPORTED_BORDERS)
	if len(eventInfos) == 0 {
		logrus.Info("No new events to process.")
		return nil
	}
	logrus.Infof("Got %d events to process", len(eventInfos))

	if err := dao.SaveEventInfos(eventInfos); err != nil {
		return errors.New("save event infos: " + err.Error())
	}

	eventIds := make([]int, 0, len(eventInfos)+1)
	eventIds = append(eventIds, latest.EventId)
	for _, info := range eventInfos {
		eventIds = append(eventIds, info.EventId)
		if info.EventId > latest.EventId {
			latest = info
		}
	}

	borderInfos := collectBorderInfos(client, eventIds, SURPPORTED_BORDERS, SURPPORTED_BORDER_TYPE)
	if err := dao.SaveBorderInfos(borderInfos); err != nil {
		return errors.New("save border infos: " + err.Error())
	}
	if err := dao.SaveLatestEventInfo(latest); err != nil {
		return errors.New("save latest event info: " + err.Error())
	}
	logrus.Info("Job completed successfully.")
	return nil
}

func collectBorderInfos(
	matsuriClient matsuri.MatsuriClient,
	eventIds []int,
	supportedBorders []int,
	supportedBorderType models.EventRankingType,
) []models.BorderInfo {
	var borderInfos []models.BorderInfo

	for _, eventId := range eventIds {
		for _, border := range supportedBorders {
			rankingLogs, err := matsuriClient.GetEventRankingLogs(
				eventId,
				supportedBorderType,
				border,
				nil,
			)
			if err != nil {
				logrus.Warnf("Failed to get ranking logs for event %d with border: %d : %s", eventId, border, err.Error())
				continue
			}

			for _, log := range rankingLogs {
				for _, data := range log.Data {
					borderInfo := models.BorderInfo{
						EventId:      eventId,
						Border:       border,
						RankingType:  supportedBorderType,
						Score:        data.Score,
						AggregatedAt: data.AggregatedAt,
					}
					borderInfos = append(borderInfos, borderInfo)
				}
			}
		}
	}
	return borderInfos
}

func collectEventInfos(
	matsuriClient matsuri.MatsuriClient,
	events []models.Event,
	maxEventId int,
	supportedBorders []int,
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

		if utils.IsSubset(supportedBorders, borders.EventPoint) &&
			models.EventType(event.Type) != models.Anniversary {
			eventInfo := models.EventInfo{
				EventId:           event.Id,
				EventType:         models.EventType(event.Type),
				InternalEventType: models.ToInternalEventType(event),
				StartAt:           event.Schedule.BeginAt,
				EndAt:             event.Schedule.EndAt,
				BoostAt:           event.Schedule.BoostBeginAt,
			}
			logrus.Infof("Collected info for event %d", event.Id)
			eventInfos = append(eventInfos, eventInfo)
		} else {
			logrus.Infof("Skipped event %d with name: %s", event.Id, event.Name)
		}
	}

	return eventInfos
}
