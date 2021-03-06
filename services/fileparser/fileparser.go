package fileparser

import (
	"encoding/json"
	"github.com/roumpid/rabbitmq-goperf/config"
	"github.com/roumpid/rabbitmq-goperf/log"
	"github.com/roumpid/rabbitmq-goperf/rmq"
	"github.com/roumpid/rabbitmq-goperf/utils"
	"github.com/streadway/amqp"
)

var exchangeName = ""
var serviceName = "user_queue"
var rpcMethodName = "parse"

var (
	ch      *rmq.Channel
	replyTo string
	chMsgs  <-chan amqp.Delivery
	err     error
)

type Reply struct {
	ReplyTo string
	ChMsgs  <-chan amqp.Delivery
}

var sp = rmq.SetupParams{
	ServiceName:  serviceName,
	ExchangeName: exchangeName,
	ExchangeType: "topic",
	QueueName:    "rpc_" + serviceName + utils.GetUDID(),
	RoutingKey:   "rpc_server_queue",
}

type Kwargs struct {
}


type FileParserMessage struct {
	Kwargs Kwargs   `json:"kwargs"`
	Args   []string `json:"args"`
}

type RpcMessage struct {
	Type string `json:"type"`
}

func init() {

	settings, err := config.GetConfig()
	failOnError(err, "Unable to get required configuration")

	amqpClient := rmq.Client{
		Host:     settings.RabbitMQ.Host,
		Port:     settings.RabbitMQ.Port,
		Username: settings.RabbitMQ.Username,
		Password: settings.RabbitMQ.Password,
		Vhost:	  settings.RabbitMQ.Vhost,
	}

	err = amqpClient.NewConnection()
	failOnError(err, "Unable to connect to RabbitMQ")

	//defer amqpClient.CloseConnection()
	ch, err = amqpClient.SetupChannel(sp)
	failOnError(err, "Unable to Setup Channel")
}

// CreateQueue ...
func (r *Reply) CreateQueue() {
	var err error
	r.ReplyTo, err = ch.CreateReplyQueue(sp.ExchangeName)
	failOnError(err, "Unable to create reply queue")

	r.ChMsgs, err = ch.ConsumeMessage(r.ReplyTo, "")
	failOnError(err, "Registering consumer failed")
}

// Parse ...
func (r *Reply) Parse() {


	//fileParserMessage := FileParserMessage{
	//	Kwargs: Kwargs{},
	//	Args:   []string{"c29tZSBmaWxlIHRleHQ=", "", "TXT"},
	//}

	rpcMessage := &RpcMessage{Type: "calendar"}
	body, err := json.Marshal(rpcMessage)
	failOnError(err, "Unable to create file parser message body")

	uuid := utils.GetUDID()
	table := amqp.Table{}
	table["correlation_id"] = uuid
	table["reply_to"] = r.ReplyTo

	err = ch.PublishMessageAsRPC(sp.ExchangeName, sp.RoutingKey, amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		CorrelationId:   uuid,
		DeliveryMode:    2,
		Priority:        0,
		ReplyTo:         r.ReplyTo,
		Body:            body,
		Headers:         table,
	})
	log.Info("body is :  %s and reply_to is : %s", body, r.ReplyTo )
	failOnError(err, "Publishing message failed")

	go func() {
		msg, _ := rmq.GetMessageForCorrelationID(r.ChMsgs, uuid)
		log.Info("Msg response for correlation id %s -> %s reply_to", uuid, msg)
	}()

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
