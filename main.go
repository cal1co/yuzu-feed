package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/cal1co/yuzu-feed/middleware"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	redis "github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

// TODO: Configure adding posts from post service to feeds <- COMPLETED
// TODO: Configure JWT so users aren't hard coded <- COMPLETED

// TODO: refactor code - handlers file, delete repeated code, general sepearation of concerns
// TODO: Configure expiration of redis items
// TODO: Separate redis cache's for different services

type Post struct {
	ID          gocql.UUID `json:"post_id"`
	UserID      int        `json:"user_id"`
	PostContent string     `json:"post_content"`
	CreatedAt   time.Time  `json:"created_at"`
	Likes       int        `json:"like_count"`
	Comments    int        `json:"comments_count"`
}

var (
	kafkaBroker   = "localhost:9092"
	kafkaTopic    = "user_posts"
	redisAddr     = "localhost:6379"
	redisPassword = ""
	redisDB       = 0
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var (
	kafkaWriter *kafka.Writer
	redisClient *redis.Client
	cqlSession  *gocql.Session
	psql        *sql.DB
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

	r.POST("/post", func(c *gin.Context) {
		handlePost(c)
	})

	authenticatedRoutes := r.Group("/v1")
	authenticatedRoutes.Use(middleware.AuthMiddleware())
	{

		authenticatedRoutes.GET("/connect", func(c *gin.Context) {
			handleRead(c)
		})

		authenticatedRoutes.POST("/addpoststofeed", func(c *gin.Context) {
			handleAddUserPostsToFeed(c)
		})

		authenticatedRoutes.GET("/feed/:pageId", func(c *gin.Context) {
			handleFeed(c)
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

type FollowFeedPayload struct {
	Follow_id int
}

func handleFeed(c *gin.Context) {
	userID, exists := c.Get("user_id")
	if !exists {
		ThrowUserIDExtractError(c)
		return
	}
	uid := int(userID.(float64))
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	postIDs, err := redisClient.ZRevRange(ctx, fmt.Sprintf("feed:%d", uid), int64(start), int64(end-1)).Result()
	if err != nil {
		log.Fatal(err)
	}

	for _, postID := range postIDs {
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
	userID, exists := c.Get("user_id")
	if !exists {
		ThrowUserIDExtractError(c)
		return
	}
	uid := int(userID.(float64))
	var followedUser FollowFeedPayload
	if err := c.BindJSON(&followedUser); err != nil {
		fmt.Println(err)
	}
	query := cqlSession.Query("SELECT post_id, created_at FROM posts WHERE user_id = ? AND created_at <= ?", followedUser.Follow_id, time.Now())

	iter := query.Iter()
	var postId string
	var created_at time.Time

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	feedKey := fmt.Sprintf("feed:%d", uid)
	fmt.Println(feedKey)
	for iter.Scan(&postId, &created_at) {
		redisClient.ZAdd(ctx, feedKey, redis.Z{
			Score:  float64(created_at.Unix()),
			Member: postId,
		})
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func handlePost(c *gin.Context) {
	var post Post
	if err := c.BindJSON(&post); err != nil {
		fmt.Println(err)
	}
	postToKafka(post, "user_posts")
	fanoutPost(post)
}
func postToKafka(post Post, topic string) error {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer writer.Close()

	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(strconv.Itoa(post.UserID)),
			Value: []byte(post.ID.String()),
		},
	)
	if err != nil {
		fmt.Printf("failed to write messages: %s", err)
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("failed to close writer: %s", err)
	}

	log.Printf("Message posted to Kafka successfully")
	return nil
}
func fanoutPost(post Post) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	active, err := getActiveFollowers(post.UserID)
	if err != nil {
		return fmt.Errorf("failed to get active followers")
	}
	for i := 0; i < len(active); i++ {
		redisClient.ZAdd(ctx, fmt.Sprintf("feed:%d", active[i]), redis.Z{
			Score:  float64(post.CreatedAt.Unix()),
			Member: post.ID,
		})
	}
	return nil
}

func getActiveFollowers(userID int) ([]int, error) {
	var active_users []int
	query := "SELECT f.follower_id FROM followers f INNER JOIN user_activity ua ON f.follower_id = ua.user_id WHERE f.following_id = $1 AND ua.last_active >= CURRENT_TIMESTAMP - INTERVAL '12 hours';"
	rows, err := psql.Query(query, userID)
	if err != nil {
		return []int{0}, fmt.Errorf("failed to execute the query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var follower int
		err := rows.Scan(&follower)
		if err != nil {
			return []int{0}, fmt.Errorf("failed to scan row: %v", err)
		}
		active_users = append(active_users, follower)
	}
	fmt.Println("active users:", active_users)
	return active_users, nil
}
func ThrowUserIDExtractError(c *gin.Context) {
	c.JSON(http.StatusNotFound, "Couldn't extract uid")
	c.AbortWithStatus(http.StatusBadRequest)
}
func handleRead(c *gin.Context) {
	userID, exists := c.Get("user_id")
	if !exists {
		ThrowUserIDExtractError(c)
		return
	}
	uid := int(userID.(float64))
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("Failed to upgrade the connection to WebSocket:", err)
		return
	}
	defer conn.Close()
	followingList, err := getFollowing(uid)
	if err != nil {
		fmt.Println(err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	readKafka("user_posts", followingList, conn, uid)
}

func createTopic(topic string, partitions int, replicationFactor int) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partitions)
	if err != nil {
		panic(err.Error())
	}

	log.Printf("Topic '%s' created successfully", topic)
	return conn, nil
}

func readKafka(topic string, following map[int]int, conn *websocket.Conn, userID int) {
	stopChan := make(chan struct{})

	brokers := []string{"localhost:9092"}
	config := kafka.ReaderConfig{
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

		lastOffset, err := getOffset(userID)
		if err != nil {
			log.Println("Failed to get offset from Redis:", err)
			lastOffset = 0
		}

		reader.SetOffset(lastOffset)

		for {
			select {
			case <-sigchan:
				return
			case <-stopChan:
				return
			default:
				m, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Println("Error reading message:", err)
					continue
				}
				posterId, err := strconv.Atoi(string(m.Key))
				if err != nil {
					fmt.Println(err)
				}
				if _, exists := following[posterId]; exists {
					err := conn.WriteMessage(websocket.TextMessage, m.Value)
					if err != nil {
						log.Println("Failed to write message to WebSocket:", err)
						conn.Close()
						stopChan <- struct{}{}
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					redisClient.ZAdd(ctx, fmt.Sprintf("feed:%d", userID), redis.Z{
						Score:  float64(m.Time.Unix()),
						Member: m.Value,
					})
					storeOffset(userID, m.Offset+1)
				}
			}
		}
	}()

	_, _, err := conn.ReadMessage()
	if err != nil {
		log.Println("Failed to read message from WebSocket:", err)
	}
	stopChan <- struct{}{}
	wg.Wait()
}
func storeOffset(userID int, offset int64) error {
	ctx := context.Background()

	err := redisClient.Set(ctx, fmt.Sprintf("offset:user:%d", userID), offset, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func getOffset(userID int) (int64, error) {
	ctx := context.Background()

	offset, err := redisClient.Get(ctx, fmt.Sprintf("offset:user:%d", userID)).Int64()
	if err != nil {
		return 0, err
	}

	return offset, nil
}
func processMessage(m kafka.Message) {
	fmt.Println("PROCESSING MESSAGE", m)
}

func getFollowing(userID int) (map[int]int, error) {
	query := "SELECT following_id FROM followers WHERE follower_id = $1"

	rows, err := psql.Query(query, userID)
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
