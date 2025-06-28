package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	endpoint := os.Getenv("R2_ENDPOINT")
	accessKeyId := os.Getenv("R2_ACCESS_KEY_ID")
	accessKeySecret := os.Getenv("R2_SECRET_ACCESS_KEY")

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	bucket := "mltd-border-predict"
	prefix := "" // all objects
	localBase := "cmd/r2data"

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			panic(err)
		}
		for _, obj := range page.Contents {
			key := *obj.Key
			fmt.Println("Downloading:", key)
			out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
			})
			if err != nil {
				fmt.Printf("Failed to download %s: %v\n", key, err)
				continue
			}
			localPath := filepath.Join(localBase, filepath.FromSlash(key))
			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				fmt.Printf("Failed to create dir for %s: %v\n", localPath, err)
				continue
			}
			f, err := os.Create(localPath)
			if err != nil {
				fmt.Printf("Failed to create file %s: %v\n", localPath, err)
				out.Body.Close()
				continue
			}
			_, err = io.Copy(f, out.Body)
			out.Body.Close()
			f.Close()
			if err != nil {
				fmt.Printf("Failed to write file %s: %v\n", localPath, err)
			}
		}
	}
	fmt.Println("Done.")
}
