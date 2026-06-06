package kafka

import "go.uber.org/zap"

type ConsumerGroup struct {
	log    *zap.Logger
	topics []*Topic
}

func NewConsumerGroup(log *zap.Logger) *ConsumerGroup {
	return &ConsumerGroup{
		log: log,
	}
}

func (g *ConsumerGroup) Register(topic *Topic) {
	g.topics = append(g.topics, topic)
}

func (g *ConsumerGroup) Start() {

}
