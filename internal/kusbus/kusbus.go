package kusbus

import (
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

/**
 * Handler for a content-type.
 */
type ContentTypeHandler interface {
	FromBytes([]byte) (interface{}, error)
	ToBytes(interface{}) ([]byte, error)
}

type RpcHandlerFunc = func(rpcType string, rpc interface{}) (interface{}, error)
type BroadcastHandlerFunc = func(topic string, msg interface{})

/**
 * Transform amqp.Delivery to RpcHandler / BroadcastHandler call.
 *
 * This struct contains fields for purely computational dispatch and serialization logic.
 */
type HandlerTable struct {
	/**
	 * Handlers for different content.
	 */
	ContentTypeHandlerMap map[string]ContentTypeHandler

	/**
	 * Handlers for RPC.  Indexed by rpcType.
	 */
	RpcHandlerMap map[string]RpcHandlerFunc

	/**
	 * Handlers for broadcast.  Indexed by topic.
	 */
	BroadcastHandlerMap map[string]BroadcastHandlerFunc

	/**
	 * Number of RPC consumers
	 */
	RpcNumConsumers int

	/**
	 * Number of broadcast consumers
	 */
	BroadcastNumConsumers int
}

/**
 * Koinos microservice bus.
 *
 * - Each RPC message has an rpcType
 * - Queue for RPC of type T is kept in queue named `koins_rpc_T`
 * - RPC messages per node type
 * - Single global exchange for events, all node types
 */
type Kusbus struct {
	/**
	 * Remote address to connect to.
	 */
	Address string

	/**
	 * Handlers for RPC and broadcast.
	 */
	Handlers HandlerTable
}

/**
 * Encapsulates all the connection-specific queue.
 */

type KusbusConnection struct {
	AmqpConn *amqp.Connection
	AmqpChan *amqp.Channel

	/**
	 * RpcRecvQueue is a durable competing-consumer queue shared by all Kusbus nodes.
	 */
	RpcRecvQueue *amqp.Queue

	/**
	 * BroadcastRecvQueue is an exclusive private queue for the current Kusbus node.
	 * BroadcastRecvQueue will receive messages from the broadcast topic exchange.
	 */
	BroadcastRecvQueue *amqp.Queue

	/**
	 * Handlers for RPC and broadcast.
	 */
	Handlers *HandlerTable

	NotifyClose chan *amqp.Error
}

func NewKusbus(addr string) *Kusbus {
	kusbus := Kusbus{}
	kusbus.Address = addr

	kusbus.Handlers.ContentTypeHandlerMap = make(map[string]ContentTypeHandler)
	kusbus.Handlers.RpcHandlerMap = make(map[string]RpcHandlerFunc)
	kusbus.Handlers.BroadcastHandlerMap = make(map[string]BroadcastHandlerFunc)

	kusbus.Handlers.RpcNumConsumers = 1
	kusbus.Handlers.BroadcastNumConsumers = 1

	return &kusbus
}

/**
 * The Start() function starts the connection loop.
 */
func (kusbus *Kusbus) Start() {
	go kusbus.ConnectLoop()
}

/**
 * Set the content type handler for a content type.
 */
func (kusbus *Kusbus) SetContentTypeHandler(contentType string, handler ContentTypeHandler) {
	kusbus.Handlers.ContentTypeHandlerMap[contentType] = handler
}

/**
 * Set the RPC handler for an rpcType.
 */
func (kusbus *Kusbus) SetRpcHandler(rpcType string, handler RpcHandlerFunc) {
	kusbus.Handlers.RpcHandlerMap[rpcType] = handler
}

/**
 * Set the broadcast handler for a topic.
 */
func (kusbus *Kusbus) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	kusbus.Handlers.BroadcastHandlerMap[topic] = handler
}

/**
 * Set the number of consumers for queues.
 *
 * This sets the number of parallel goroutines that consume the respective AMQP queues.
 * Must be called before Connect().
 */
func (kusbus *Kusbus) SetNumConsumers(rpcNumConsumers int, broadcastNumConsumers int) {
	kusbus.Handlers.RpcNumConsumers = rpcNumConsumers
	kusbus.Handlers.BroadcastNumConsumers = broadcastNumConsumers
}

/**
 * ConnectLoop() is the main entry point.
 */
func (kusbus *Kusbus) ConnectLoop() *KusbusConnection {
	RETRY_MIN_DELAY := 1
	RETRY_MAX_DELAY := 25
	RETRY_DELAY_PER_RETRY := 2
	for {
		retryCount := 0
		log.Printf("Connecting to AMQP server %v\n", kusbus.Address)

		var conn *KusbusConnection
		for {
			conn = kusbus.NewKusbusConnection()
			err := conn.Open(kusbus.Address, &kusbus.Handlers)
			if err == nil {
				break
			}
			delay := RETRY_MIN_DELAY + RETRY_DELAY_PER_RETRY*retryCount
			if delay > RETRY_MAX_DELAY {
				delay = RETRY_MAX_DELAY
			}
			select {
			/*
			   // TODO: Add quit channel for clean termination
			   case <-kusbus.quitChan:
			      return
			*/
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			}
		}
		select {
		/*
		   // TODO: Add quit channel for clean termination
		   case <-kusbus.quitChan:
		      return
		*/
		case <-conn.NotifyClose:
		}
	}
}

func (kusbus *Kusbus) NewKusbusConnection() *KusbusConnection {
	conn := KusbusConnection{}
	conn.Handlers = &kusbus.Handlers
	return &conn
}

/**
 * Set all fields to nil or default values.
 */
func (c *KusbusConnection) reset() {
	c.AmqpConn = nil
	c.AmqpChan = nil
}

/**
 * Close the connection.
 */
func (c *KusbusConnection) Close() error {
	amqpConn := c.AmqpConn
	c.reset()

	if amqpConn == nil {
		return nil
	}
	return amqpConn.Close()
}

/**
 * Attempt to connect.
 *
 * Return error if connection attempt fails (i.e., no retry)
 */
func (c *KusbusConnection) Open(addr string, handlers *HandlerTable) error {
	if (c.AmqpConn != nil) || (c.AmqpChan != nil) {
		return errors.New("Attempted to reuse KusbusConnection")
	}
	var err error = nil

	closeIfError := func() {
		if err != nil {
			c.Close()
		}
	}
	defer closeIfError()

	// We keep the connection and channel local until the connection's fully set up.
	log.Printf("Dialing AMQP server %s\n", addr)
	c.AmqpConn, err = amqp.Dial(addr)
	if err != nil {
		log.Printf("AMQP error dialing server: %v\n", err)
		return err
	}
	c.AmqpChan, err = c.AmqpConn.Channel()
	if err != nil {
		log.Printf("AMQP error connecting to channel: %v\n", err)
		return err
	}

	c.NotifyClose = make(chan *amqp.Error)
	c.AmqpChan.NotifyClose(c.NotifyClose)

	err = c.AmqpChan.ExchangeDeclare(
		"koinos_event", // Name
		"topic",        // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("AMQP error calling ExchangeDeclare: %v\n", err)
		return err
	}

	for rpcType, _ := range handlers.RpcHandlerMap {
		consumers, err := c.ConsumeRpc(rpcType, handlers.RpcNumConsumers)
		if err != nil {
			return err
		}
		for _, consumer := range consumers {
			go c.ConsumeRpcLoop(consumer, handlers, rpcType, c.AmqpChan)
		}
	}

	for topic, _ := range handlers.BroadcastHandlerMap {
		consumers, err := c.ConsumeBroadcast(topic, handlers.BroadcastNumConsumers)
		if err != nil {
			return err
		}
		for _, consumer := range consumers {
			go c.ConsumeBroadcastLoop(consumer, handlers, topic)
		}
	}
	return nil
}

/**
 * Create a delivery channel for the given RpcType.
 */
func (c *KusbusConnection) ConsumeRpc(rpcType string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	rpcQueueName := "koinos_rpc_" + rpcType

	_, err := c.AmqpChan.QueueDeclare(
		rpcQueueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("AMQP error calling QueueDeclare: %v", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			rpcQueueName, // Queue
			"",           // Consumer
			false,        // AutoAck
			false,        // Exclusive
			false,        // NoLocal
			false,        // NoWait
			nil,          // Arguments
		)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

/**
 * Create a delivery channel for given broadcast topic.
 *
 * Returned channels are competing consumers on a single AMQP queue.
 */
func (c *KusbusConnection) ConsumeBroadcast(topic string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	broadcastQueue, err := c.AmqpChan.QueueDeclare(
		"",
		false, // Durable
		false, // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("AMQP error calling QueueDeclare: %v\n", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			broadcastQueue.Name, // Queue
			"",                  // Consumer
			false,               // AutoAck
			false,               // Exclusive
			false,               // NoLocal
			false,               // NoWait
			nil,                 // Arguments
		)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

/**
 * Consumption loop for RPC.  Normally, the caller would run this function in a goroutine.
 */
func (c *KusbusConnection) ConsumeRpcLoop(consumer <-chan amqp.Delivery, handlers *HandlerTable, rpcType string, RespChan *amqp.Channel) {
	log.Printf("Enter ConsumeRpcLoop\n")
	for delivery := range consumer {
		output_pub := handlers.HandleRpcDelivery(rpcType, &delivery)

		err := RespChan.Publish(
			"",               // Exchange
			delivery.ReplyTo, // Routing key (channel name for default exchange)
			false,            // Mandatory
			false,            // Immediate
			*output_pub,      // Message
		)
		if err != nil {
			log.Printf("Couldn't deliver message, error is %v\n", err)
			// TODO: Should an error close the connection?
		}
	}
	log.Printf("Exit ConsumeRpcLoop\n")
}

/**
 * Consumption loop for broadcast.  Normally, the caller would run this function in a goroutine.
 */
func (c *KusbusConnection) ConsumeBroadcastLoop(consumer <-chan amqp.Delivery, handlers *HandlerTable, topic string) {
	log.Printf("Enter ConsumeBroadcastLoop\n")
	for delivery := range consumer {
		handlers.HandleBroadcastDelivery(topic, &delivery)
	}
	log.Printf("Exit ConsumeBroadcastLoop\n")
}

/**
 * Handle a single RPC delivery.
 *
 * Parse request Delivery using ContentTypeHandler, dispatch to Handler function,
 * serialize response Publishing using ContentTypeHandler.
 */
func (handlers *HandlerTable) HandleRpcDelivery(rpcType string, delivery *amqp.Delivery) *amqp.Publishing {
	// TODO:  Proper RPC error handling

	cth := handlers.ContentTypeHandlerMap[delivery.ContentType]
	if cth != nil {
		log.Printf("Unknown ContentType\n")
		return nil
	}
	input, err := cth.FromBytes(delivery.Body)
	if err != nil {
		log.Printf("Couldn't deserialize rpc input\n")
		return nil
	}
	handler := handlers.RpcHandlerMap[rpcType]
	output, err := handler(rpcType, input)
	if err != nil {
		log.Printf("Error in RPC handler\n")
		return nil
	}
	output_bytes, err := cth.ToBytes(output)
	if err != nil {
		log.Printf("Couldn't serialize rpc output\n")
		return nil
	}
	output_pub := amqp.Publishing{
		DeliveryMode:  amqp.Transient,
		Timestamp:     time.Now(),
		ContentType:   delivery.ContentType,
		CorrelationId: delivery.CorrelationId,
		Body:          output_bytes,
	}
	return &output_pub
}

/**
 * Handle a single broadcast delivery.
 */
func (handlers *HandlerTable) HandleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	cth := handlers.ContentTypeHandlerMap[delivery.ContentType]
	if cth != nil {
		log.Printf("Unknown ContentType\n")
		return
	}
	input, err := cth.FromBytes(delivery.Body)
	if err != nil {
		log.Printf("Couldn't deserialize broadcast\n")
		return
	}
	handler := handlers.BroadcastHandlerMap[topic]
	handler(delivery.RoutingKey, input)
}
