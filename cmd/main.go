package main

import (
	"flag"

	"github.com/alceccentric/matsurihi-cron/internal/dao"
	"github.com/alceccentric/matsurihi-cron/internal/jobs"
	"github.com/alceccentric/matsurihi-cron/internal/matsuri"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	mode := flag.String("mode", "local", "DAO mode: local or r2")
	flag.Parse()

	client := matsuri.NewMatsurihiMeClient(matsuri.BASE_URL_V2)
	var borderDAO dao.DAO

	switch *mode {
	case "local":
		borderDAO = dao.NewLocalDAO("data", "border_info", "evnent_info", "metadata")
	case "r2":
		borderDAO = dao.NewR2DAO("mltd-border-predict", "normal/border_info", "normal/evnent_info", "normal/metadata")
	default:
		logrus.Fatalf("Unknown mode: %s", *mode)
	}

	if err := jobs.RunSync(client, borderDAO); err != nil {
		logrus.Fatal("Job failed: ", err)
	}
}
