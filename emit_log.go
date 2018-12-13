package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing-contrib/go-amqp/amqptracer"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/streadway/amqp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// Init open trace
	t := opentracer.New(
		tracer.WithAgentAddr("127.0.0.1:8126"),
		tracer.WithServiceName("emit"),
		tracer.WithGlobalTag("env", "nicolas-pylon"),
	)

	opentracing.SetGlobalTracer(t)
	defer tracer.Stop()

	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	body := bodyFrom(os.Args)

	// TRACING STUFF
	//If we were to use the topic
	sp := opentracing.StartSpan("emit")

	//panic: assignment to entry in nil map
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
		Headers:     amqp.Table{},
	}

	if err := amqptracer.Inject(sp, msg.Headers); err != nil {
		fmt.Println(err)
	}

	time.Sleep(2 * time.Second)
	fmt.Println("parent" + msg.Headers["x-datadog-parent-id"].(string))
	fmt.Println("trace" + msg.Headers["x-datadog-trace-id"].(string))
	sp.SetTag("traceID", strconv.FormatUint(sp.Context().(ddtrace.SpanContext).TraceID(), 10))
	defer sp.Finish()
	// FINISH TRACING

	err = ch.Publish(
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		msg,
	)
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
