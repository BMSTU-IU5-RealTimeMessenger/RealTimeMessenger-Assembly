package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

type Segment struct {
	Data   string `json:"data"`
	Time   int64  `json:"time"`
	Number int    `json:"number"`
	Count  int    `json:"count"`
}

type Message struct {
	Data  string `json:"data"`
	Error string `json:"error"`
}

type Server struct {
	HTTPClient  *http.Client
	Destination string
	Topic       string
	Period      time.Duration
	Timeout     time.Duration
	messages    map[int64][]Segment
	lastUpdate  map[int64]time.Time
	KafkaAddr   string
}

func NewServer() (*Server, error) {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file")
	}

	kafkaAddr := os.Getenv("KAFKA_ADDR")
	topic := os.Getenv("TOPIC")
	destination := os.Getenv("APPLICATION_LAYER_ADDR")
	period, err := strconv.Atoi(os.Getenv("ASSEMBLY_PERIOD"))
	if err != nil {
		return nil, err
	}
	timeout, err := strconv.Atoi(os.Getenv("TIMEOUT"))
	if err != nil {
		return nil, err
	}

	return &Server{
		HTTPClient:  http.DefaultClient,
		Topic:       topic,
		Destination: destination,
		Period:      time.Duration(period),
		Timeout:     time.Duration(timeout),
		messages:    make(map[int64][]Segment),
		lastUpdate:  make(map[int64]time.Time),
		KafkaAddr:   kafkaAddr,
	}, nil
}

func (s *Server) Run() error {
	log.Println("starting server")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer([]string{s.KafkaAddr}, config)
	if err != nil {
		return err
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panic(err)
		}
	}()
	partitionConsumer, err := consumer.ConsumePartition(s.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(s.Period * time.Second)
	defer ticker.Stop()

	log.Println("Server is running")

	for {
		select {
		case <-ticker.C:
			s.processAvailableSegments(partitionConsumer)
		case <-signals:
			return nil
		case err := <-partitionConsumer.Errors():
			log.Printf("Consumer error: %v", err)
		}
	}
}

func (s *Server) processAvailableSegments(consumer sarama.PartitionConsumer) {
	log.Println("Reading new segments")
	for {
		select {
		case msg := <-consumer.Messages():
			var segment Segment
			if err := json.Unmarshal(msg.Value, &segment); err != nil {
				log.Printf("Error unmarshalling message: %v", err)
				continue
			}
			log.Println("Got segment:", segment)

			s.messages[segment.Time] = append(s.messages[segment.Time], segment)
			s.lastUpdate[segment.Time] = time.Now()
		default:
			for key, segments := range s.messages {
				if len(segments) == segments[0].Count {
					message := AssembleMessage(segments)
					log.Println("Assemble whole message:", message)
					if err := s.SendMessage(message); err != nil {
						log.Println(http.StatusInternalServerError, err)
						return
					}
					delete(s.messages, key)
					continue
				}
				if time.Now().After(s.lastUpdate[key].Add(s.Timeout * time.Second)) {
					message := Message{Error: "message is partially corrupted"}
					if err := s.SendMessage(message); err != nil {
						log.Println(http.StatusInternalServerError, err)
						return
					}
					delete(s.messages, key)
				}
			}
			return
		}
	}
}

func AssembleMessage(segments []Segment) Message {
	var data string
	for _, segment := range segments {
		data += segment.Data
	}
	return Message{
		Data: data,
	}
}

func (s *Server) SendMessage(message Message) error {
	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "http://"+s.Destination, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("Unexpected response status: " + resp.Status)
	}
	return nil
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Fatalln(err)
		return
	}
	
	err = server.Run()
	if err != nil {
		log.Fatalln(err)
		return
	}
}
