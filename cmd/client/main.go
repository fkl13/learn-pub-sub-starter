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
	fmt.Println("Starting Peril client...")

	const rabbitmqUrl = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitmqUrl)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ server: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connected to RabbitMQ successfully")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get username: %v", err)
	}
	gameState := gamelogic.NewGameState(username)

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.TransientQueue, HandlerPause(gameState))
	if err != nil {
		log.Fatalf("failed to subscribe to pause: %v", err)
	}

	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gameState.GetUsername())
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, moveQueueName, routing.ArmyMovesPrefix+".*", pubsub.TransientQueue, HandlerMove(gameState))
	if err != nil {
		log.Fatalf("failed to subscribe to army moves: %v", err)
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}
		switch inputs[0] {
		case "spawn":
			err := gameState.CommandSpawn(inputs)
			if err != nil {
				fmt.Printf("could not spawn unit: %v", err)
			}
		case "move":
			move, err := gameState.CommandMove(inputs)
			if err != nil {
				fmt.Printf("could not move unit: %v", err)
				continue
			}
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+move.Player.Username, move)
			if err != nil {
				fmt.Printf("failed to publish move: %v", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)

		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			fmt.Println("Exiting game")
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func HandlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func HandlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
}
