package dao

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/alceccentric/matsurihi-cron/utils"
	"github.com/gocarina/gocsv"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type LocalDAO struct {
	outputPath         string
	borderInfoDir      string
	eventInfoDir       string
	latestEventInfoDir string
}

func NewLocalDAO(outputPath, borderInfoDir, eventInfoDir, metadataInfoDir string) *LocalDAO {
	var err error
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(path.Join(outputPath, borderInfoDir)))
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(path.Join(outputPath, eventInfoDir)))
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(path.Join(outputPath, metadataInfoDir)))

	if err != nil {
		logrus.WithError(err).Fatal("Failed to create output directories")
	}

	return &LocalDAO{
		outputPath:         outputPath,
		borderInfoDir:      borderInfoDir,
		eventInfoDir:       eventInfoDir,
		latestEventInfoDir: metadataInfoDir,
	}
}

func (u *LocalDAO) GetLatestEventInfo() (models.EventInfo, error) {
	filepath := path.Join(u.outputPath, u.latestEventInfoDir, LATEST_EVENT_BORDER_INFO_FILE)
	if !utils.LocalFileExists(filepath) {
		return models.EventInfo{}, nil
	}
	var latestInfo models.EventInfo
	if err := utils.ReadJSONFile(filepath, &latestInfo); err != nil {
		return models.EventInfo{}, err
	}
	return latestInfo, nil
}
func (u *LocalDAO) SaveEventInfos(eventInfos []models.EventInfo) error {
	latestInfo, err := u.GetLatestEventInfo()
	if err != nil {
		return err
	}
	filepath := path.Join(u.outputPath, u.eventInfoDir, EVENT_INFO_FILENAME)
	isEmpty := latestInfo.EventId == 0
	if isEmpty {
		logrus.Infof("Saving %d event infos to %s for the first time", len(eventInfos), filepath)
		return save(filepath, eventInfos, false)
	} else {
		logrus.Infof("Saving %d event infos to %s, filtering based on latest event ID: %d", len(eventInfos), filepath, latestInfo.EventId)
		return save(filepath, filterEventInfos(eventInfos, latestInfo), true)
	}

}

func (u *LocalDAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	latestInfo, err := u.GetLatestEventInfo()
	if err != nil {
		return err
	}
	borderInfosByBorderGroupKey := groupByEventIdAndBorder(borderInfos)
	isEmpty := latestInfo.EventId == 0
	if isEmpty {
		var err error
		for key, infos := range borderInfosByBorderGroupKey {
			filepath := path.Join(u.outputPath, u.borderInfoDir, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, key.EventId, key.Border))
			logrus.Infof("Saving %d border infos for event ID %d and border %d to %s for the first time", len(infos), key.EventId, key.Border, filepath)
			err = multierr.Append(err, save(filepath, infos, false))
		}
		return err
	} else {
		var err error
		for key, infos := range borderInfosByBorderGroupKey {
			if key.EventId < latestInfo.EventId {
				logrus.Warnf("Skipping border info for event ID %d, as it is older than the latest event ID %d", key.EventId, latestInfo.EventId)
				continue
			}
			filepath := path.Join(u.outputPath, u.borderInfoDir, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, key.EventId, key.Border))
			// Always replace border info file (of each event and border) completely, which means it assumes the invoker fetch entire border logs for each event and border.
			logrus.Infof("Saving %d border infos for event ID %d and border %d to %s, filtering based on latest event ID: %d", len(infos),
				key.EventId, key.Border, filepath, latestInfo.EventId)
			err = multierr.Append(err, save(filepath, infos, false))
		}
		return err
	}
}

func save[T any](path string, infos []T, append bool) error {
	var file *os.File
	var err error

	flag := os.O_CREATE | os.O_WRONLY
	if append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	if len(infos) == 0 {
		logrus.Warnf("No data to save to %s", path)
		return nil
	}

	if file, err = os.OpenFile(path, flag, 0644); err != nil {
		return err
	}
	defer file.Close()

	writeHeaders := !append || !utils.LocalFileExists(path)
	if writeHeaders {
		if err = gocsv.MarshalFile(infos, file); err != nil {
			return err
		}
	} else {
		csvWriter := csv.NewWriter(file)
		defer csvWriter.Flush()
		if err = gocsv.MarshalCSVWithoutHeaders(infos, csvWriter); err != nil {
			return err
		}
	}

	return nil
}
