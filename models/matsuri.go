package models

import (
	"strings"
	"time"
)

// The event type enum represents the type of an event in my app only.
type InternalEventType int

const (
	_ InternalEventType = iota
	Internal_ShowTime
	Internal_MilliColle1
	Internal_Theater // Only for プラチナスターシアター in my app; In PrincessAPI, this type covers [15, 20] as well.
	Internal_Tour    // Only for プラチナスターツアー in my app; In PrincessAPI, this type covers [21, 23] as well.
	Internal_Anniversary
	Internal_Working
	Internal_AprilFool
	Internal_GameCorner
	Internal_MilliColle2
	Internal_TwinStage1
	Internal_Tune
	Internal_TwinStage2
	Internal_Tale // Only for プラチナスターテール in my app; In PrincessAPI, this type covers [24, 25] as well.
	Internal_TalkParty
	Internal_TheaterSpecial   // プラチナスターシアタースペシャル
	Internal_Tiara            // プラチナスターティアラ
	Internal_Trust13          // プラチナスタートラスト13
	Internal_TrustBOT         // プラチナスタートラストBOT
	Internal_TrustSpecial     // プラチナスタートラストスペシャル
	Internal_Trust            // プラチナスタートラスト
	Internal_TourSpecial      // プラチナスターツアースペシャル
	Internal_TourBingoSpecial // プラチナスターツアービンゴスペシャル
	Internal_TourBingo        // プラチナスターツアービンゴ
	Internal_Team             // プラチナスターチーム
	Internal_Time             // プラチナスタータイム
)

// The event type enum represents the type of an event in PrincessAPI.
type EventType int

const (
	_ EventType = iota
	ShowTime
	MilliColle1
	Theater
	Tour
	Anniversary
	Working
	AprilFool
	GameCorner
	MilliColle2
	TwinStage1
	Tune
	TwinStage2
	Tale
	TalkParty
)

// ToInternalEventType converts a PrincessAPI event type to an internal event type.
func ToInternalEventType(event Event) InternalEventType {
	switch event.Type {
	case 3:
		switch {
		case strings.HasPrefix(event.Name, "プラチナスターシアタースペシャル"):
			return Internal_TheaterSpecial
		case strings.HasPrefix(event.Name, "プラチナスターティアラ"):
			return Internal_Tiara
		case strings.HasPrefix(event.Name, "プラチナスタートラスト13"):
			return Internal_Trust13
		case strings.HasPrefix(event.Name, "プラチナスタートラストBOT"):
			return Internal_TrustBOT
		case strings.HasPrefix(event.Name, "プラチナスタートラストスペシャル"):
			return Internal_TrustSpecial
		case strings.HasPrefix(event.Name, "プラチナスタートラスト"):
			return Internal_Trust
		default:
			return Internal_Theater
		}
	case 4:
		switch {
		case strings.HasPrefix(event.Name, "プラチナスターツアースペシャル"):
			return Internal_TourSpecial
		case strings.HasPrefix(event.Name, "プラチナスターツアービンゴスペシャル"):
			return Internal_TourBingoSpecial
		case strings.HasPrefix(event.Name, "プラチナスターツアービンゴ"):
			return Internal_TourBingo
		default:
			return Internal_Tour
		}
	case 13:
		switch {
		case strings.HasPrefix(event.Name, "プラチナスターチーム"):
			return Internal_Team
		case strings.HasPrefix(event.Name, "プラチナスタータイム"):
			return Internal_Time
		default:
			return Internal_Tale
		}
	default:
		return InternalEventType(event.Type)
	}
}

type EventSortType string

const (
	IdAsc       EventSortType = "id"
	IdDesc      EventSortType = "id!"
	TypeAsc     EventSortType = "type"
	TypeDesc    EventSortType = "type!"
	BeginAtAsc  EventSortType = "beginAt"
	BeginAtDesc EventSortType = "beginAt!"
)

type EventsOptions struct {
	At       time.Time
	Types    []EventType
	OrderBys []EventSortType
}

type Event struct {
	Id         int    `json:"id"`
	Type       int    `json:"type"`
	AppealType int    `json:"appealType"`
	Name       string `json:"name"`
	Schedule   struct {
		BeginAt      time.Time `json:"beginAt"`
		EndAt        time.Time `json:"endAt"`
		PageOpenedAt time.Time `json:"pageOpenedAt"`
		PageClosedAt time.Time `json:"pageClosedAt"`
		BoostBeginAt time.Time `json:"boostBeginAt"`
		BoostEndAt   time.Time `json:"boostEndAt"`
	} `json:"schedule"`
	Item struct {
		Name      string `json:"name"`
		ShortName string `json:"shortName"`
	} `json:"item"`
}

type IdolPointBorders struct {
	IdolId  int   `json:"idolId"`
	Borders []int `json:"borders"`
}

type EventRankingBorders struct {
	EventPoint  []int              `json:"eventPoint"`
	HighScore   []int              `json:"highScore"`
	LoungePoint []int              `json:"loungePoint"`
	IdolPoint   []IdolPointBorders `json:"idolPoint"`
}

type EventRankingLogsOptions struct {
	Since      time.Time
	IfNonMatch string
}

type EventRankingLog struct {
	Rank int `json:"rank"`
	Data []struct {
		Score        int       `json:"score"`
		AggregatedAt time.Time `json:"aggregatedAt"`
	} `json:"data"`
}

type EventRankingType string

const (
	EventPoint     EventRankingType = "eventPoint"
	HighScore      EventRankingType = "highScore"
	highScore2     EventRankingType = "highScore2"
	HighScoreTotal EventRankingType = "highScoreTotal"
	LoungePoint    EventRankingType = "loungePoint"
	IdolPoint      EventRankingType = "idolPoint"
)
