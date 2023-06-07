package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	redis "github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

// POST:
//    user posts to a topic
// FEED:
//    user selects posts from relevant topics
//    adds posts to feed
//    returns paginated feed (~20 posts)
//    posts are stored simply as id's to deal with deleted posts (subject to change this feature)
//    "online" users get posts in real time or from polling every once-in-a-while so they recieve updates for recents posts

var (
	kafkaBroker   = "localhost:9092"
	kafkaTopic    = "user_posts"
	redisAddr     = "localhost:6379"
	redisPassword = ""
	redisDB       = 0
)

var (
	kafkaWriter *kafka.Writer
	redisClient *redis.Client
)

type Post struct {
	UserID   int    `json:"userId"`
	PostID   int    `json:"postId"`
	Content  string `json:"content"`
	DateTime string `json:"dateTime"`
}

func init() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	redisClient = rdb
}
func main() {

	r := gin.Default()
	_, err := createTopic("user_posts", 0, 0)
	if err != nil {
		fmt.Println(err)
	}
	r.POST("/", func(c *gin.Context) {
		handlePost(c)
	})

	r.GET("/", func(c *gin.Context) {
		handleRead(c)
	})

	go func() {
		if err := r.Run(":8081"); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	log.Println("Server started on port 8080")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down server...")

	log.Println("Server shutdown complete")
}

type Payload struct {
	User         int
	Post_content string
}

func handlePost(c *gin.Context) {
	var payload Payload
	if err := c.BindJSON(&payload); err != nil {
		fmt.Println(err)
	}
	fmt.Println("payload: ", payload)
	postToKafka(payload.User, payload.Post_content, "user_posts")
}

func handleRead(c *gin.Context) {
	readKafka("user_posts", []int{1})
}

// createTopic creates a Kafka topic with the given topic name, partition count, and replication factor.
func createTopic(topic string, partitions int, replicationFactor int) (*kafka.Conn, error) {
	// Create an admin client
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partitions)
	if err != nil {
		panic(err.Error())
	}

	log.Printf("Topic '%s' created successfully", topic)
	return conn, nil
}

func postToKafka(userID int, content string, topic string) error {
	// Create a Kafka writer
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	// Produce a message to the Kafka topic
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(strconv.Itoa(userID)),
			Value: []byte(content),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	log.Printf("Message posted to Kafka successfully")
	return nil
}

// func readKafka(topic string) {
// 	r := kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:   []string{"localhost:9092", "localhost:9093", "localhost:9094"},
// 		Topic:     topic,
// 		Partition: 0,
// 		MaxBytes:  10e6,
// 	})
// 	r.SetOffset(42)

// 	for {
// 		m, err := r.ReadMessage(context.Background())
// 		if err != nil {
// 			break
// 		}
// 		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
// 	}

//		if err := r.Close(); err != nil {
//			log.Fatal("failed to close reader:", err)
//		}
//	}

func readKafka(topic string, following []int) {
	brokers := []string{"localhost:9092"}
	config := kafka.ReaderConfig{
		// GroupID:         """,
		Brokers:         brokers,
		Topic:           topic,
		MinBytes:        10e3,
		MaxBytes:        10e6,
		MaxWait:         1 * time.Second,
		ReadLagInterval: -1,
	}

	reader := kafka.NewReader(config)
	defer reader.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-sigchan:
				return
			default:
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Println("Error reading message:", err)
					continue
				}
				fmt.Println(m)
				// userID := extractUserIDFromMessage(m)
				// if isFollowing(userID, following) {
				// processMessage(m)
				// }
			}
		}
	}()

	wg.Wait()
}
