package dao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gocarina/gocsv"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

type R2DAO struct {
	s3                 S3Uploader
	bucketName         string
	borderInfoPrefix   string
	eventInfoPrefix    string
	metadataInfoPrefix string
}

func NewR2DAO(bucketName, borderInfoPrefix, eventInfoPrefix, metadataInfoPrefix string) *R2DAO {
	return &R2DAO{
		s3:                 initS3Client(),
		bucketName:         bucketName,
		borderInfoPrefix:   borderInfoPrefix,
		eventInfoPrefix:    eventInfoPrefix,
		metadataInfoPrefix: metadataInfoPrefix,
	}
}

func NewR2DAOWithClient(bucketName, borderInfoPrefix, eventInfoPrefix, metadataInfoPrefix string, s3Client S3Uploader) *R2DAO {
	return &R2DAO{
		s3:                 s3Client,
		bucketName:         bucketName,
		borderInfoPrefix:   borderInfoPrefix,
		eventInfoPrefix:    eventInfoPrefix,
		metadataInfoPrefix: metadataInfoPrefix,
	}
}

func (u *R2DAO) GetLatestEventInfo() (models.EventInfo, error) {
	key := path.Join(u.metadataInfoPrefix, LATEST_EVENT_BORDER_INFO_FILE)
	resp, err := u.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(u.bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		return models.EventInfo{}, err
	}
	defer resp.Body.Close()

	var latestInfo models.EventInfo
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&latestInfo); err != nil {
		return models.EventInfo{}, err
	}

	return latestInfo, nil
}

func (u *R2DAO) SaveEventInfos(eventInfos []models.EventInfo) error {
	latestInfo, err := u.GetLatestEventInfo()
	if err != nil {
		return err
	}
	key := path.Join(u.eventInfoPrefix, EVENT_INFO_FILENAME)
	isEmpty := latestInfo.EventId == 0
	if isEmpty {
		logrus.Infof("Saving %d event infos to bucket: %s with key: %s for the first time",
			len(eventInfos), u.bucketName, key)
		return writeCSVToR2(u.s3, u.bucketName, key, eventInfos, false)
	} else {
		logrus.Infof("Saving %d event infos to bucket: %s with key: %s, appending to existing data",
			len(eventInfos), u.bucketName, key)
		return writeCSVToR2(u.s3, u.bucketName, key, eventInfos, true)
	}
}

func (u *R2DAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	latestInfo, err := u.GetLatestEventInfo()
	if err != nil {
		return err
	}
	borderInfosByBorderGroupKey := groupByEventIdAndBorder(borderInfos)
	isEmpty := latestInfo.EventId == 0
	if isEmpty {
		var err error
		for group, infos := range borderInfosByBorderGroupKey {
			key := path.Join(u.borderInfoPrefix, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, group.EventId, group.Border))
			err = multierr.Append(err, writeCSVToR2(u.s3, u.bucketName, key, infos, false))
		}
		return err
	} else {
		var err error
		for group, infos := range borderInfosByBorderGroupKey {
			if group.EventId < latestInfo.EventId {
				logrus.Warnf("Skipping border info for event ID %d, as it is older than the latest event ID %d", group.EventId, latestInfo.EventId)
				continue
			}
			key := path.Join(u.borderInfoPrefix, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, group.EventId, group.Border))
			// Always replace border info file (of each event and border) completely, which means it assumes the invoker fetch all the available border logs for each event and border.
			logrus.Infof("Saving %d border infos for event ID %d and border %d to bucket: %s with key: %s, appending to existing data",
				len(infos), group.EventId, group.Border, u.bucketName, key)
			err = multierr.Append(err, writeCSVToR2(u.s3, u.bucketName, key, infos, false))
		}
		return err
	}
}

func initS3Client() *s3.Client {
	// Load .env only for local dev
	_ = godotenv.Load()

	endpoint := os.Getenv("R2_ENDPOINT")
	accessKeyId := os.Getenv("R2_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("R2_SECRET_ACCESS_KEY")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		log.Fatal(err)
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
func writeCSVToR2[T any](
	client S3Uploader,
	bucket, key string,
	records []T,
	appendMode bool,
) error {
	// Marshal all records to CSV bytes (includes header)
	csvBytes, err := gocsv.MarshalBytes(records)
	if err != nil {
		return fmt.Errorf("failed to marshal csv: %w", err)
	}

	var fullData []byte

	if appendMode {
		// Check if object exists by trying to get it
		resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})

		if err != nil {
			var nsk *types.NoSuchKey
			if errors.As(err, &nsk) {
				// File doesn't exist → write full CSV (header + data)
				fullData = csvBytes
			} else {
				return fmt.Errorf("failed to get object: %w", err)
			}
		} else {
			defer resp.Body.Close()
			existingData, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("failed to read existing object: %w", err)
			}

			// Remove header line from new CSV before appending
			csvStr := string(csvBytes)
			idx := strings.Index(csvStr, "\n")
			if idx == -1 {
				return fmt.Errorf("csv data malformed, no newline found")
			}
			dataWithoutHeader := csvBytes[idx+1:]

			// Append new CSV data (no header) to existing content
			fullData = append(existingData, dataWithoutHeader...)
		}
	} else {
		// Overwrite mode → write entire CSV including header
		fullData = csvBytes
	}

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(fullData),
	})
	return err
}
