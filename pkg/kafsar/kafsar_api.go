package kafsar

type Server interface {
	Auth(username string, password string) (bool, error)

	AuthTopic(username string, password, topic string) (bool, error)

	AuthTopicGroup(username string, password, consumerGroup string) (bool, error)
}
