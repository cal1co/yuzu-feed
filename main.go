package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/cal1co/yuzu-feed/handlers"
	"github.com/cal1co/yuzu-feed/middleware"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	redis "github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

// TODO: Configure expiration of redis items
// TODO: Separate redis cache's for different services

var (
	kafkaBroker   = "localhost:9092"
	kafkaTopic    = "user_posts"
	redisAddr     = "localhost:6379"
	redisPassword = ""
	redisDB       = 0
	kafkaWriter   *kafka.Writer
	redisClient   *redis.Client
	cqlSession    *gocql.Session
	psql          *sql.DB
)

func init() {
	loadEnv()

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "user_posts"
	var err error
	cqlSession, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	redisClient = rdb
	psql, err = sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}
}
func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}
func main() {
	_, err := createTopic("user_posts", 0, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	r := gin.Default()

	r.Use(middleware.RateLimiterMiddleware())
	config := cors.DefaultConfig()
	config.AllowMethods = []string{"GET", "POST", "DELETE", "OPTIONS"}
	config.AddAllowHeaders("Authorization")
	config.AllowOrigins = []string{"http://localhost:5173"}

	r.Use(cors.New(config))
	r.POST("/post", func(c *gin.Context) {
		handlers.HandlePost(c, redisClient, psql)
	})
	r.GET("/connect/:token", func(c *gin.Context) {
		handlers.HandleRead(c, psql, redisClient)
	})

	authenticatedRoutes := r.Group("/v1")
	authenticatedRoutes.Use(middleware.AuthMiddleware())
	{

		authenticatedRoutes.POST("/addpoststofeed", func(c *gin.Context) {
			handlers.HandleAddUserPostsToFeed(c, cqlSession, redisClient)
		})

		authenticatedRoutes.GET("/feed/:page", func(c *gin.Context) {
			handlers.HandleFeed(c, redisClient, cqlSession, psql)
		})

	}

	go func() {
		if err := r.Run(":8081"); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	defer psql.Close()
	defer cqlSession.Close()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down server...")

	log.Println("Server shutdown complete")
}

func createTopic(topic string, partitions int, replicationFactor int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partitions)
	if err != nil {
		panic(err.Error())
	}

	log.Printf("Topic '%s' created successfully", topic)
	return conn, nil
}
