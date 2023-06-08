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
	"github.com/gocql/gocql"
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
	cqlSession  *gocql.Session
)

type Post struct {
	UserID   int    `json:"userId"`
	PostID   int    `json:"postId"`
	Content  string `json:"content"`
	DateTime string `json:"dateTime"`
}

func init() {
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
}
func main() {
	defer cqlSession.Close()

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

	r.POST("/addpoststofeed", func(c *gin.Context) {
		handleAddUserPostsToFeed(c)
	})

	r.GET("/feed", func(c *gin.Context) {
		handleFeed(c)
	})

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

type FollowFeedPayload struct {
	Id       int
	Follower int
}

func handleFeed(c *gin.Context) {

}

func handleAddUserPostsToFeed(c *gin.Context) {
	// select all from cassandra
	// add posts to user feed as sorted set
	var followedUser FollowFeedPayload
	if err := c.BindJSON(&followedUser); err != nil {
		fmt.Println(err)
	}
	query := cqlSession.Query("SELECT post_id, created_at FROM posts WHERE user_id = ? AND created_at <= ?", followedUser.Id, time.Now())

	iter := query.Iter()
	var postId string
	var created_at time.Time

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// exists, err := redisClient.ZScore(ctx).Result()
	feedKey := fmt.Sprintf("feed:%d", followedUser.Follower)
	fmt.Println(feedKey)
	for iter.Scan(&postId, &created_at) {
		// fmt.Println(postId, created_at)
		redisClient.ZAdd(ctx, feedKey, redis.Z{
			Score:  float64(created_at.Unix()),
			Member: postId,
		})
	}

	// Check for any errors during iteration
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
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
