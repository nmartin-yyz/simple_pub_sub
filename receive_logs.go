package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
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
	t := opentracer.New(
		tracer.WithAgentAddr("127.0.0.1:8126"),
		tracer.WithServiceName("receive"),
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

	q, err := ch.QueueDeclare(
		"logs", // name
		false,  // durable
		false,  // delete when unused
		true,   // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
			spCtx, _ := amqptracer.Extract(d.Headers)
			sp := opentracing.StartSpan(
				"ConsumeMessage",
				opentracing.ChildOf(spCtx),
			)
			ctx := opentracing.ContextWithSpan(context.Background(), sp)
			fmt.Println("Headers")
			fmt.Println("parentID : " + d.Headers["x-datadog-parent-id"].(string))
			fmt.Println("trace    : " + d.Headers["x-datadog-trace-id"].(string))

			fmt.Println("new created Span")
			fmt.Println("traceID  : " + strconv.FormatUint(sp.Context().(ddtrace.SpanContext).TraceID(), 10))
			fmt.Println("SpanID   : " + strconv.FormatUint(sp.Context().(ddtrace.SpanContext).SpanID(), 10))
			sp.SetTag("traceID", strconv.FormatUint(sp.Context().(ddtrace.SpanContext).TraceID(), 10))
			time.Sleep(2 * time.Second)
			// sp.Finish()
			// PUB AGAIN
			//If we were to use the topic
			// spCtx, _ := amqptracer.Extract(d.Headers)
			// spPub := opentracing.StartSpan(
			// 	"ConsumeMessage",
			// 	opentracing.ChildOf(spCtx),
			// )
			spPub := opentracing.SpanFromContext(ctx)
			//panic: assignment to entry in nil map
			msg := amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(d.Body),
				Headers:     amqp.Table{},
			}

			if err := amqptracer.Inject(spPub, msg.Headers); err != nil {
				fmt.Println(err)
			}

			fmt.Println("parent" + msg.Headers["x-datadog-parent-id"].(string))
			fmt.Println("trace" + msg.Headers["x-datadog-trace-id"].(string))
			spPub.SetTag("traceID", strconv.FormatUint(sp.Context().(ddtrace.SpanContext).TraceID(), 10))
			spPub.Finish()
			// sp.Finish()

			err = ch.Publish(
				"logs2", // exchange
				"",      // routing key
				false,   // mandatory
				false,   // immediate
				msg,
			)
			failOnError(err, "Failed to publish a message")

			log.Printf(" [x] Sent %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
