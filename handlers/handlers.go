package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Post struct {
	ID               gocql.UUID `json:"post_id"`
	UserID           int        `json:"user_id"`
	PostContent      string     `json:"post_content"`
	CreatedAt        time.Time  `json:"created_at"`
	Likes            int        `json:"like_count"`
	Comments         int        `json:"comments_count"`
	Username         string     `json:"username"`
	Liked            bool       `json:"liked"`
	DisplayName      string     `json:"display_name"`
	ProfileImageData string     `json:"profile_image_data"`
	ProfileImage     string     `json:"profile_image"`
}

type PostUser struct {
	ID               int
	Username         string
	DisplayName      string
	ProfileImageData string
}

func extractUserId(c *gin.Context) (int, error) {
	userID, exists := c.Get("user_id")
	if !exists {
		return 0, ThrowUserIDExtractError(c)
	}

	return int(userID.(float64)), nil
}

func addPostToRedisFeed(uid int, redisClient *redis.Client, post Post) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	redisClient.ZAdd(ctx, fmt.Sprintf("feed:%d", uid), redis.Z{
		Score:  float64(post.CreatedAt.Unix()),
		Member: post.ID,
	})
}

func HandlePost(c *gin.Context, redisClient *redis.Client, psql *sql.DB) {
	var post Post
	if err := c.BindJSON(&post); err != nil {
		fmt.Println(err)
	}
	postToKafka(post, "user_posts")
	fanoutPost(post, redisClient, psql)
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

func fanoutPost(post Post, redisClient *redis.Client, psql *sql.DB) error {

	active, err := getActiveFollowers(post.UserID, psql)
	if err != nil {
		return fmt.Errorf("failed to get active followers")
	}
	for i := 0; i < len(active); i++ {
		addPostToRedisFeed(active[i], redisClient, post)
	}
	return nil
}

func getActiveFollowers(userID int, psql *sql.DB) ([]int, error) {
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

func HandleRead(c *gin.Context, psql *sql.DB, redisClient *redis.Client) {
	txn := c.Param("token")
	uid, err := middleware.ExtractUserID(txn)
	if err != nil {
		fmt.Println("error verifying token")
		return
	}
	allowedOrigins := map[string]bool{
		"http://localhost:5173": true,
	}
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return allowedOrigins[origin]
		},
	}
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("Failed to upgrade the connection to WebSocket:", err)
		return
	}
	defer conn.Close()
	followingList, err := getFollowing(uid, psql)
	if err != nil {
		fmt.Println(err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}
	readKafka("user_posts", followingList, conn, uid, redisClient)
}

func getFollowing(userID int, psql *sql.DB) (map[int]int, error) {
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

func ThrowUserIDExtractError(c *gin.Context) error {
	c.JSON(http.StatusNotFound, "Couldn't extract uid")
	c.AbortWithStatus(http.StatusBadRequest)
	return fmt.Errorf("Error: %s", "Couldn't extract user id from token")
}

func readKafka(topic string, following map[int]int, conn *websocket.Conn, userID int, redisClient *redis.Client) {
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
	fmt.Println("READING KAFKA NOW")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		lastOffset, err := getOffset(userID, redisClient)
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
					storeOffset(userID, m.Offset+1, redisClient)
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

func storeOffset(userID int, offset int64, redisClient *redis.Client) error {
	ctx := context.Background()

	err := redisClient.Set(ctx, fmt.Sprintf("offset:user:%d", userID), offset, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func getOffset(userID int, redisClient *redis.Client) (int64, error) {
	ctx := context.Background()

	offset, err := redisClient.Get(ctx, fmt.Sprintf("offset:user:%d", userID)).Int64()
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func HandleAddUserPostsToFeed(c *gin.Context, cqlSession *gocql.Session, redisClient *redis.Client) {
	uid, err := extractUserId(c)
	if err != nil {
		return
	}
	var followedUser int
	if err := c.BindJSON(&followedUser); err != nil {
		fmt.Println(err)
	}
	query := cqlSession.Query("SELECT post_id, created_at FROM posts WHERE user_id = ? AND created_at <= ?", followedUser, time.Now())

	iter := query.Iter()
	var postId string
	var created_at time.Time

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for iter.Scan(&postId, &created_at) {
		redisClient.ZAdd(ctx, fmt.Sprintf("feed:%d", uid), redis.Z{
			Score:  float64(created_at.Unix()),
			Member: postId,
		})
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func HandleFeed(c *gin.Context, redisClient *redis.Client, cqlSession *gocql.Session, psql *sql.DB) {
	uid, err := extractUserId(c)
	if err != nil {
		fmt.Println(err)
		return
	}

	num, err := getFeedPages(uid, redisClient)
	if err != nil {
		fmt.Println(err)
	}

	page_num, err := strconv.Atoi(c.Param("page"))
	if err != nil {
		fmt.Println(err)
	}
	pageSize := 15
	fmt.Println(fmt.Sprintf("THERE ARE %d pages", int(math.Ceil(float64(num))/float64(pageSize))))
	end := page_num * pageSize
	start := end - pageSize

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	postIDs, err := redisClient.ZRevRange(ctx, fmt.Sprintf("feed:%d", uid), int64(start), int64(end-1)).Result()
	if err != nil {
		log.Fatal(err)
	}
	var posts []Post
	cachedUsers := make(map[int]PostUser)

	endpoint := fmt.Sprintf("http://localhost:8082/posts/feed/%d", uid)
	payload, err := json.Marshal(postIDs)
	res, err := http.Post(endpoint, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
	}

	var postData []Post
	err = json.Unmarshal(body, &postData)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(postData); i++ {
		_, ok := cachedUsers[postData[i].UserID]
		if !ok {
			userSearchQuery := "SELECT username, display_name, profile_image FROM users WHERE id=$1"
			rows, err := psql.Query(userSearchQuery, postData[i].UserID)
			if err != nil {
				fmt.Println("FAILED QUERY", err)
				continue
			}
			defer rows.Close()

			for rows.Next() {
				var postUser PostUser
				err := rows.Scan(&postUser.Username, &postUser.DisplayName, &postUser.ProfileImageData)
				if err != nil {
					fmt.Println("FAILED QUERY", err)
					continue
				}
				postUser.ID = postData[i].UserID
				cachedUsers[postUser.ID] = postUser
			}
		}
		postData[i].Username = cachedUsers[postData[i].UserID].Username
		postData[i].DisplayName = cachedUsers[postData[i].UserID].DisplayName
		postData[i].ProfileImageData = cachedUsers[postData[i].UserID].ProfileImageData
		posts = append(posts, postData[i])
	}

	// endpoint = "http://localhost:3000/api/auth/s3image/feed/"
	// payload, err = json.Marshal(postData)
	// res, err = http.Post(endpoint, "application/json", bytes.NewBuffer(payload))
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// defer res.Body.Close()

	// var fullPosts []Post
	// err = json.Unmarshal(body, &fullPosts)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	c.JSON(http.StatusOK, postData)
}

func getFeedPages(userId int, redisClient *redis.Client) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	totalPosts, err := redisClient.ZCard(ctx, fmt.Sprintf("feed:%d", userId)).Result()
	if err != nil {
		fmt.Println("Failed to get the total number of posts:", err)
		return 0, fmt.Errorf("failed to get the total number of posts: %w", err)
	}
	return totalPosts, nil
}
