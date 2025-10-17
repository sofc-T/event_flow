// Package jwt provides JWT generation and validation services.
package jwt

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dgrijalva/jwt-go"
)

// Service implements the ijwt.IService interface for handling JWT operations.
type Service struct {
	secretKey      string
	issuer         string
	expTime        time.Duration
	refreshExpTime time.Duration
}



// Config holds the configuration for creating a new JWT Service.
type Config struct {
	SecretKey      string
	Issuer         string
	ExpTime        time.Duration
	RefreshExpTime time.Duration
}

// New creates a new JWT Service with the given configuration.
func New(config Config) *Service {
	return &Service{
		secretKey:      config.SecretKey,
		issuer:         config.Issuer,
		expTime:        config.ExpTime,
		refreshExpTime: config.RefreshExpTime,
	}
}

// GeneratebusinessOwner implements ijwt.Service.
func (s *Service) Generate( tokenType string) (string, error) {
	email := "<user_email>"
	name := "<user_name>"
	role := "<user_role>"
	id := "<user_id>"

	var expTime time.Duration

	switch tokenType {
	case "access":
		expTime = s.expTime
	case "refresh":
		expTime = s.refreshExpTime
	default:
		expTime = time.Hour * 24 * 7
	}

	claims := jwt.StandardClaims{
		ExpiresAt: time.Now().Add(expTime).Unix(),
		Issuer:    s.issuer,
	}

	tokenClaims := jwt.MapClaims{
		"email":         email,
		"name":          name,
		"role":          role,
		"exp":           claims.ExpiresAt,
		"issuer":        claims.Issuer,	
		"id": id,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, tokenClaims)
	signedToken, err := token.SignedString([]byte(s.secretKey))
	if err != nil {
		log.Println("Error generating token:", err)
		return "", errors.New("couldn't generate token")
	}

	log.Println("Generated token:", signedToken)
	return signedToken, nil
}



func (s *Service) Decode(tokenString string) (jwt.MapClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &jwt.MapClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(s.secretKey), nil
	})

	if err != nil {
		return nil, errors.New("invalid token: " + err.Error())
	}

	claims, ok := token.Claims.(*jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	return *claims, nil
}