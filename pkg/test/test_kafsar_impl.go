package test

type KafsarImpl struct {
}

func (k KafsarImpl) Auth(username string, password string) (bool, error) {
	return true, nil
}

func (k KafsarImpl) AuthTopic(username string, password, topic string) (bool, error) {
	return true, nil
}

func (k KafsarImpl) AuthTopicGroup(username string, password, consumerGroup string) (bool, error) {
	return true, nil
}
