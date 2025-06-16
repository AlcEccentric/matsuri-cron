package cmd

import (
	"github.com/alceccentric/matsurihi-cron/internal/dao"
	"github.com/alceccentric/matsurihi-cron/internal/jobs"
	"github.com/alceccentric/matsurihi-cron/internal/matsuri"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.InfoLevel)

	client := matsuri.NewMatsurihiMeClient(matsuri.BASE_URL_V2)
	dao := dao.NewLocalDAO("data", "border_info", "evnent_info", "metadata")
	if err := jobs.RunSync(client, dao); err != nil {
		logrus.Fatal("Job failed: ", err)
	}
	logrus.Info("Job completed successfully")
}
