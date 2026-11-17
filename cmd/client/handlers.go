package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gamestate *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gamestate.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerArmyMoves(gamestate *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gamestate.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gamestate.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gamestate.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Printf("Error publishing war recognition: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("error: unknown move outcome")
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gamestate *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(recog gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gamestate.HandleWar(recog)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return publishGameLog(gamestate, ch, fmt.Sprintf("%s won a war against %s", winner, loser))
		case gamelogic.WarOutcomeYouWon:
			return publishGameLog(gamestate, ch, fmt.Sprintf("%s won a war against %s", winner, loser))
		case gamelogic.WarOutcomeDraw:
			return publishGameLog(gamestate, ch, fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser))
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(gamestate *gamelogic.GameState, ch *amqp.Channel, message string) pubsub.AckType {
	defer fmt.Print("> ")
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gamestate.GetUsername(), routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    gamestate.GetUsername(),
	})
	if err != nil {
		fmt.Printf("Error writing game log: %v\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
