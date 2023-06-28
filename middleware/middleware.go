package middleware

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	"golang.org/x/time/rate"
)

func RateLimiterMiddleware() gin.HandlerFunc {
	limiter := rate.NewLimiter(1, 5)

	return func(c *gin.Context) {
		if limiter.Allow() == false {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "Too many requests"})
			return
		}
		c.Next()
	}
}
func verifyToken(tokenString string) (*jwt.Token, error) {
	return jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(os.Getenv("SECRET_TOKEN")), nil
	})
}
func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			fmt.Println("NO AUTH HEADER", c.Request)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			return
		}
		tokenString := strings.Replace(authHeader, "Bearer ", "", 1)

		token, err := verifyToken(tokenString)
		if err != nil {
			log.Printf("error: %s", err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization token"})
			return
		}
		userID := token.Claims.(jwt.MapClaims)["id"].(float64)
		c.Set("user_id", userID)
		_, exists := c.Get("user_id")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization token"})
			return
		}
		c.Next()
	}
}
func ExtractUserID(inputToken string) (int, error) {
	tokenString := strings.Replace(inputToken, "Bearer ", "", 1)
	token, err := verifyToken(tokenString)
	if err != nil {
		log.Printf("error: %s", err)
		return 0, fmt.Errorf("error: invalid authorization token")
	}
	return int(token.Claims.(jwt.MapClaims)["id"].(float64)), nil
}
