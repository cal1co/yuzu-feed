package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
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

func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	defer cqlSession.Close()

	r := gin.Default()

	_, err := createTopic("user_posts", 0, 0)
	if err != nil {
		fmt.Println(err)
	}

	loadEnv()
	list, err := getFollowing(28)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("FOLLOW LIST", list)

	r.POST("/", func(c *gin.Context) {
		handlePost(c)
	})

	r.GET("/", func(c *gin.Context) {
		handleRead(c, list)
	})

	r.POST("/addpoststofeed", func(c *gin.Context) {
		handleAddUserPostsToFeed(c)
	})

	r.GET("/feed/:pageId", func(c *gin.Context) {
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

type FeedGetterPayload struct {
	Id int
}

func handleFeed(c *gin.Context) {
	num, err := getFeedPages(0)
	if err != nil {
		fmt.Println(err)
	}
	page_num, err := strconv.Atoi(c.Param("pageId"))
	if err != nil {
		fmt.Println(err)
	}
	pageSize := 5
	fmt.Println(fmt.Sprintf("THERE ARE %d pages", int(math.Ceil(float64(num))/float64(pageSize))))
	end := page_num * pageSize
	start := end - pageSize
	var user FeedGetterPayload
	if err := c.BindJSON(&user); err != nil {
		fmt.Println(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	postIDs, err := redisClient.ZRevRange(ctx, fmt.Sprintf("feed:%d", user.Id), int64(start), int64(end-1)).Result()
	if err != nil {
		log.Fatal(err)
	}

	// Process the retrieved post IDs (e.g., fetch post details from another data source)
	for _, postID := range postIDs {
		// Process each post ID
		fmt.Println("Post ID:", postID)
	}
}
func getFeedPages(userId int) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	totalPosts, err := redisClient.ZCard(ctx, fmt.Sprintf("feed:%d", userId)).Result()
	if err != nil {
		fmt.Println("Failed to get the total number of posts:", err)
		return 0, fmt.Errorf("failed to get the total number of posts: %w", err)
	}
	return totalPosts, nil
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
	feedKey := fmt.Sprintf("feed:%d", followedUser.Follower)
	fmt.Println(feedKey)
	for iter.Scan(&postId, &created_at) {
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

func handleRead(c *gin.Context, list map[int]int) {
	readKafka("user_posts", list)
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

func readKafka(topic string, following map[int]int) {
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
				userID, err := strconv.Atoi(string(m.Key))
				if err != nil {
					fmt.Println(err)
				}
				if _, exists := following[userID]; exists {
					processMessage(m)
				}
			}
		}
	}()

	wg.Wait()
}

func processMessage(m kafka.Message) {
	fmt.Println("PROCESSING MESSAGE", m)
}

func getFollowing(userID int) (map[int]int, error) {
	db, err := sql.Open("postgres", os.Getenv("DB_URL"))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	query := "SELECT following_id FROM followers WHERE follower_id = $1"

	rows, err := db.Query(query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to execute the query: %v", err)
	}
	defer rows.Close()

	users := make(map[int]int)
	for rows.Next() {
		var followingID int
		err := rows.Scan(&followingID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		users[followingID] = followingID
		fmt.Println(users)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred during iteration: %v", err)
	}
	return users, nil
}
