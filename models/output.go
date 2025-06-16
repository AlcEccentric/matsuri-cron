package models

import "time"

type BorderInfo struct {
	EventId      int              `csv:"event_id"`
	Border       int              `csv:"border"`
	RankingType  EventRankingType `csv:"ranking_type"`
	AggregatedAt time.Time        `csv:"aggregated_at"`
	Score        int              `csv:"score"`
}

type EventInfo struct {
	EventId           int               `csv:"event_id"`
	EventType         EventType         `csv:"event_type"`
	InternalEventType InternalEventType `csv:"internal_event_type"`
	StartAt           time.Time         `csv:"start_at"`
	EndAt             time.Time         `csv:"end_at"`
	BoostAt           time.Time         `csv:"boost_at"`
}
