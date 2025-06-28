package dao

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/alceccentric/matsurihi-cron/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return models.EventInfo{}, nil
		}
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
	// Always replace event info file completely
	key := path.Join(u.eventInfoPrefix, EVENT_INFO_FILENAME)
	logrus.Infof("Saving %d event infos to bucket: %s with key: %s",
		len(eventInfos), u.bucketName, key)
	return writeCSVToR2(u.s3, u.bucketName, key, eventInfos)
}

func (u *R2DAO) SaveBorderInfos(borderInfos []models.BorderInfo) error {
	borderInfosByBorderGroupKey := groupByEventIdAndBorder(borderInfos)
	var err error
	for group, infos := range borderInfosByBorderGroupKey {
		key := path.Join(u.borderInfoPrefix, fmt.Sprintf(BORDER_INFO_FILENAME_FORMAT, group.EventId, group.IdolId, group.Border))
		err = multierr.Append(err, writeCSVToR2(u.s3, u.bucketName, key, infos))
	}
	return err
}

func (u *R2DAO) SaveLatestEventInfo(info models.EventInfo) error {
	// Always replace event info file completely
	key := path.Join(u.metadataInfoPrefix, LATEST_EVENT_BORDER_INFO_FILE)
	logrus.Infof("Saving latest event info %v to bucket: %s with key: %s", info, u.bucketName, key)
	return writeJsonToR2(u.s3, u.bucketName, key, info)
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
) error {
	csvBytes, err := gocsv.MarshalBytes(records)
	if err != nil {
		return fmt.Errorf("failed to marshal csv: %w", err)
	}
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(csvBytes),
	})
	return err
}

func writeJsonToR2(
	client S3Uploader,
	bucket, key string,
	data interface{},
) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(jsonBytes),
	})
	return err
}
