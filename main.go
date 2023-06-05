package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/gin-gonic/gin"
	redis "github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

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

func main() {
	kw := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer kw.Close()
	kafkaWriter = kw

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	redisClient = rdb

	router := gin.Default()

	// router.POST("/posts", createPostHandler)

	go func() {
		if err := router.Run(":8081"); err != nil {
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

// func createPostHandler(c *gin.Context) {
// 	var post Post
// 	if err := c.ShouldBindJSON(&post); err != nil {
// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
// 		return
// 	}

// 	// Produce the post message to Kafka topic
// 	message, err := json.Marshal(post)
// 	if err != nil {
// 		log.Printf("Failed to marshal post message: %v", err)
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
// 		return
// 	}
// 	if err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: message}); err != nil {
// 		log.Printf("Failed to produce post message to Kafka: %v", err)
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
// 		return
// 	}

// 	if err := addToUserFeed(post.UserID, post.PostID); err != nil {
// 		log.Printf("Failed to add post to user's feed: %v", err)
// 		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
// 		return
// 	}

// 	c.JSON(http.StatusOK, gin.H{"message": "Post created successfully"})
// }

// // func addToUserFeed(userID int, postID int) error {

// // }
