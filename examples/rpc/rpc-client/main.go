package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gastrodon/turnpike"
)

// callCtx returns a context bounding a single request to the router. Callers now
// govern their own timeouts; context.Background() would wait indefinitely.
func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func main() {
	turnpike.Debug()
	c, err := turnpike.NewWebsocketClient(turnpike.JSON, "ws://localhost:8000/", nil, nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	joinCtx, cancelJoin := callCtx()
	defer cancelJoin()
	_, err = c.JoinRealm(joinCtx, "turnpike.examples", nil)
	if err != nil {
		log.Fatal(err)
	}

	quit := make(chan bool)
	subCtx, cancelSub := callCtx()
	defer cancelSub()
	c.Subscribe(subCtx, "alarm.ring", nil, func([]interface{}, map[string]interface{}) {
		fmt.Println("The alarm rang!")
		c.Close()
		quit <- true
	})
	fmt.Print("Enter the timer duration: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		log.Fatalln("reading stdin:", err)
	}
	text := scanner.Text()
	if duration, err := strconv.Atoi(text); err != nil {
		log.Fatalln("invalid integer input:", err)
	} else {
		callContext, cancelCall := callCtx()
		defer cancelCall()
		if _, err := c.Call(callContext, "alarm.set", nil, []interface{}{duration}, nil); err != nil {
			log.Fatalln("error setting alarm:", err)
		}
	}
	<-quit
}
