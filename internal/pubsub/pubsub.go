package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonStr, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ctx := context.Background()
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonStr,
	}
	return ch.PublishWithContext(ctx, exchange, key, false, false, msg)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	durable := simpleQueueType == DurableQueue

	autoDelete, exclusive := false, false
	if simpleQueueType == TransientQueue {
		autoDelete, exclusive = true, true
	}

	table := amqp.Table{"x-dead-letter-exchange": routing.ExchangePerilDeadLetter}
	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, table)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	consumeChan, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for m := range consumeChan {
			var data T
			err := json.Unmarshal(m.Body, &data)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			ackttype := handler(data)
			switch ackttype {
			case Ack:
				m.Ack(false)
				fmt.Println("message ack")
			case NackRequeue:
				m.Nack(false, true)
				fmt.Println("message nack requeue")
			case NackDiscard:
				m.Nack(false, false)
				fmt.Println("message nack discard")
			}

		}
	}()
	return nil
}
