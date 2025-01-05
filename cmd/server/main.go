package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const rabbitmqUrl = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitmqUrl)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ server: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ")

	publishChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ channel: %v", err)
	}

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".",
		pubsub.DurableQueue,
		HandlerLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "pause":
			fmt.Println("Pausing game")
			data := routing.PlayingState{
				IsPaused: true,
			}
			err = pubsub.PublishJSON(publishChannel, routing.ExchangePerilDirect, routing.PauseKey, data)
			if err != nil {
				log.Printf("could not publish message: %v", err)
			}
		case "resume":
			fmt.Println("Resume game")
			data := routing.PlayingState{
				IsPaused: false,
			}
			err = pubsub.PublishJSON(publishChannel, routing.ExchangePerilDirect, routing.PauseKey, data)
			if err != nil {
				log.Printf("could not publish message: %v", err)
			}
		case "quit":
			fmt.Println("Exiting game")
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func HandlerLog() func(routing.GameLog) pubsub.Acktype {
	return func(gamelog routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
