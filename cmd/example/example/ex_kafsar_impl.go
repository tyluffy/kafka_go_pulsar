package main

type ExampleKafsarImpl struct {
}

func (e ExampleKafsarImpl) Auth(username string, password string) (bool, error) {
	return true, nil
}

func (e ExampleKafsarImpl) AuthTopic(username, password, topic string) (bool, error) {
	return true, nil
}

func (e ExampleKafsarImpl) AuthTopicGroup(username string, password, consumerGroup string) (bool, error) {
	return true, nil
}
