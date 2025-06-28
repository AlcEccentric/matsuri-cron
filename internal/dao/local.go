package dao

import (
	"fmt"
	"os"
	"path"

	"github.com/alceccentric/matsurihi-cron/internal/utils"
	"github.com/alceccentric/matsurihi-cron/models"
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
	filepath := path.Join(u.outputPath, u.eventInfoDir, EVENT_INFO_FILENAME)
	logrus.Infof("Saving %d event infos to %s for the first time", len(eventInfos), filepath)
	return saveCSV(filepath, eventInfos)

}

func (u *LocalDAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	borderInfosByBorderGroupKey := groupByEventIdAndBorder(borderInfos)
	var err error
	for key, infos := range borderInfosByBorderGroupKey {
		filepath := path.Join(u.outputPath, u.borderInfoDir, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, key.EventId, key.IdolId, key.Border))
		logrus.Infof("Saving %d border infos for event ID %d and border %d to %s", len(infos), key.EventId, key.Border, filepath)
		err = multierr.Append(err, saveCSV(filepath, infos))
	}
	return err
}

func (u *LocalDAO) SaveLatestEventInfo(info models.EventInfo) error {
	filepath := path.Join(u.outputPath, u.latestEventInfoDir, LATEST_EVENT_BORDER_INFO_FILE)
	logrus.Infof("Saving latest event info %v to %s", info, filepath)
	return saveJson(filepath, info, false)
}

func saveJson(path string, data interface{}, pretty bool) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", path, err)
	}
	defer file.Close()
	if err := utils.WriteJSONFile(file, data, pretty); err != nil {
		return fmt.Errorf("failed to write JSON to file %s: %w", path, err)
	}
	return nil
}

func saveCSV[T any](path string, infos []T) error {
	var file *os.File
	var err error

	flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if len(infos) == 0 {
		logrus.Warnf("No data to save to %s", path)
		return nil
	}

	if file, err = os.OpenFile(path, flag, 0644); err != nil {
		return err
	}
	defer file.Close()

	if err = gocsv.MarshalFile(infos, file); err != nil {
		return err
	}

	return nil
}
