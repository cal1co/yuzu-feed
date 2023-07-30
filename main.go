package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/cal1co/yuzu-feed/handlers"
	"github.com/cal1co/yuzu-feed/middleware"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	redis "github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

// TODO: Configure expiration of redis items
// TODO: Separate redis cache's for different services

var (
	// kafkaBroker   = "localhost:9092"
	kafkaTopic    = "user_posts"
	redisAddr     = "yuzu-feeds:6379"
	redisPassword = ""
	redisDB       = 0
	kafkaWriter   *kafka.Writer
	redisClient   *redis.Client
	cqlSession    *gocql.Session
	psql          *pgxpool.Pool
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
	psql, err = pgxpool.New(context.Background(), os.Getenv("DB_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
}
func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func MigrateActivityToPSQL() {
	ctx := context.Background()

	iter := redisClient.Scan(ctx, 0, "user:*:lastActive", 0).Iterator()

	for iter.Next(ctx) {
		key := iter.Val()

		userIDStr := strings.Split(key, ":")[1]
		userID, err := strconv.Atoi(userIDStr)
		if err != nil {
			log.Printf("Failed to extract user ID from Redis key %s: %v", key, err)
			continue
		}
		lastActiveStr, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			log.Printf("Failed to retrieve last active timestamp for user ID %d: %v", userID, err)
			continue
		}

		lastActiveTimestamp, err := strconv.ParseInt(lastActiveStr, 10, 64)
		if err != nil {
			log.Printf("Failed to parse last active timestamp for user ID %d: %v", userID, err)
			continue
		}
		lastActive := time.Unix(lastActiveTimestamp, 0)

		conn, err := psql.Acquire(context.Background())
		if err != nil {
			fmt.Println(500, "Failed to acquire database connection")
			return
		}

		currentTime := time.Now().UTC()

		thresholdDuration := 30 * time.Minute

		if currentTime.Sub(lastActive) <= thresholdDuration {
			if _, err := conn.Exec(context.Background(), "INSERT INTO user_activity (user_id, last_active) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET last_active = EXCLUDED.last_active", userID, lastActive); err != nil {
				log.Printf("Failed to insert user activity for user ID %d: %v", userID, err)
			}
		} else {
			if _, err := redisClient.Del(ctx, key).Result(); err != nil {
				log.Printf("Failed to delete user ID %d from Redis cache: %v", userID, err)
			}
		}
		defer conn.Release()

	}
	if err := iter.Err(); err != nil {
		log.Printf("Failed to retrieve keys from Redis: %v", err)
	}
}

func main() {

	defer cqlSession.Close()
	defer psql.Close()

	_, err := createTopic("user_posts", 0, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	go func() {
		for {
			MigrateActivityToPSQL()
			time.Sleep(time.Minute)
		}
	}()

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

		authenticatedRoutes.GET("/feed/post/user/follow/:id", func(c *gin.Context) {
			handlers.HandleAddUserPostsToFeed(c, cqlSession, redisClient)
		})

		authenticatedRoutes.GET("/feed/post/user/unfollow/:id", func(c *gin.Context) {
			handlers.HandlerRemoveUserPostsFromFeed(c, cqlSession, redisClient)
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
