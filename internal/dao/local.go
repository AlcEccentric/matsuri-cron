package dao

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/alceccentric/matsurihi-cron/utils"
	"github.com/gocarina/gocsv"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type LocalDAO struct {
	outputPath      string
	borderInfoDir   string
	eventInfoDir    string
	metadataInfoDir string
}

func NewLocalDAO(outputPath, borderInfoDir, eventInfoDir, metadataInfoDir string) *LocalDAO {
	var err error
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(outputPath+"/"+borderInfoDir))
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(outputPath+"/"+eventInfoDir))
	err = multierr.Append(err, utils.CreateDirectoryIfNotExists(outputPath+"/"+metadataInfoDir))

	if err != nil {
		logrus.WithError(err).Fatal("Failed to create output directories")
	}

	return &LocalDAO{
		outputPath:      outputPath,
		borderInfoDir:   borderInfoDir,
		eventInfoDir:    eventInfoDir,
		metadataInfoDir: metadataInfoDir,
	}
}

func (u *LocalDAO) GetMetadataInfo() (models.LatestEventBorderInfo, error) {
	path := u.outputPath + "/" + u.metadataInfoDir + "/latest_event_border_info.json"
	if !utils.LocalFileExists(path) {
		return models.LatestEventBorderInfo{}, nil
	}
	var latestInfo models.LatestEventBorderInfo
	if err := utils.ReadJSONFile(path, &latestInfo); err != nil {
		return models.LatestEventBorderInfo{}, err
	}
	return latestInfo, nil
}
func (u *LocalDAO) SaveEventInfos(eventInfos []models.EventInfo) error {
	latestInfo, err := u.GetMetadataInfo()
	if err != nil {
		return err
	}
	filepath := u.outputPath + "/" + u.eventInfoDir + "/" + EventInfoFileName
	isEmpty := latestInfo.EventId == 0 && len(latestInfo.LastAggregatedAtByBorder) == 0
	if isEmpty {
		logrus.Infof("Saving %d event infos to %s for the first time", len(eventInfos), filepath)
		return save(filepath, eventInfos, false)
	} else {
		logrus.Infof("Saving %d event infos to %s, filtering based on latest event ID: %d", len(eventInfos), filepath, latestInfo.EventId)
		return save(filepath, filterEventInfos(eventInfos, latestInfo), true)
	}

}

func (u *LocalDAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	latestInfo, err := u.GetMetadataInfo()
	if err != nil {
		return err
	}
	borderInfosByBorderGroupKey := groupByEventIdAndBorder(borderInfos)
	isEmpty := latestInfo.EventId == 0 && len(latestInfo.LastAggregatedAtByBorder) == 0
	if isEmpty {
		var err error
		for key, infos := range borderInfosByBorderGroupKey {
			filepath := u.outputPath + "/" + u.borderInfoDir + "/" + fmt.Sprintf(BorderInfoFileFormat, key.EventId, key.Border)
			logrus.Infof("Saving %d border infos to %s for the first time", len(infos), filepath)
			err = multierr.Append(err, save(filepath, infos, false))
		}
		return err
	} else {
		var err error
		for key, infos := range borderInfosByBorderGroupKey {
			filepath := u.outputPath + "/" + u.borderInfoDir + "/" + fmt.Sprintf(BorderInfoFileFormat, key.EventId, key.Border)
			shouldAppend := utils.LocalFileExists(filepath)
			err = multierr.Append(err, save(filepath, filterBorderInfos(infos, latestInfo), shouldAppend))
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
