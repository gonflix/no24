package service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gonflix/no24/api-server/internal/mq"
	"github.com/gonflix/no24/api-server/internal/queue"
	"github.com/gonflix/no24/api-server/internal/sse"
	"github.com/gonflix/no24/api-server/internal/types"
	"github.com/labstack/echo/v5"
)

// constants
const TOKEN_DURATION = 10 * time.Minute

// variables
var (
	jwtSecret     *rsa.PrivateKey
	tokenIssuer   string
	tokenAudience string
	tokenKeyID    string
)

func InitService(runEnv types.RunEnv, tIssuer, tAudience, tKeyID string) {
	if runEnv == "" || tIssuer == "" || tAudience == "" || tKeyID == "" {
		log.Panicf("필수 환경 변수가 설정되지 않았습니다 RUN_ENV %s, TOKEN_ISSUER %s, TOKEN_AUDIENCE %s, TOKEN_KEY_ID %s", runEnv, tIssuer, tAudience, tKeyID)
	}
	slog.Info("환경변수 RUN_ENV %s, TOKEN_ISSUER %s, TOKEN_AUDIENCE %s, TOKEN_KEY_ID %s", string(runEnv), tIssuer, tAudience, tKeyID)

	getPrivateKey, err := getPrivateKey(runEnv)
	if err != nil {
		panic(fmt.Sprintf("JWT 비밀키 로드 실패: %v", err))
	}
	jwtSecret = getPrivateKey

	tokenIssuer = tIssuer
	tokenAudience = tAudience
	tokenKeyID = tKeyID
}

func getPrivateKey(runEnv types.RunEnv) (*rsa.PrivateKey, error) {
	switch runEnv {
	case types.RunEnvLocal:
		pemStr := os.Getenv("LOCAL_JWT_PRIVATE_KEY_PEM")
		if pemStr == "" {
			return nil, fmt.Errorf("LOCAL_JWT_PRIVATE_KEY_PEM 환경변수가 설정되지 않았습니다")
		}
		return parsePEMToRSAKey(pemStr)

	case types.RunEnvProd: // 프로덕션 환경에서는 기존 키를 사용. AWS Secrets Manager에서 PEM 형식으로 로드
		privateKey, err := loadPrivateKeyFromAWS()
		if err != nil {
			return nil, fmt.Errorf("프로덕션 키 로드 실패: %w", err)
		}
		return privateKey, nil

	default:
		panic("invalid run environment")
	}
}

func loadPrivateKeyFromAWS() (*rsa.PrivateKey, error) {
	/*
		[LoadDefaultConfig가 인증 정보를 찾는 순서]

		1. 환경변수: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
		2. ~/.aws/credentials 파일 (로컬 개발 시 aws configure로 설정한 것)
		3. EC2/ECS/Lambda의 IAM Role(프로덕션 권장 — 키 없이 Role만으로 인증)
		4. 컨테이너 환경변수: AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
	*/
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ap-northeast-2"),
	)
	if err != nil {
		return nil, err
	}

	smClient := secretsmanager.NewFromConfig(cfg)

	result, err := smClient.GetSecretValue(context.TODO(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String("프로덕션jwt시크릿키ARN"), // TODO
	})
	if err != nil {
		return nil, fmt.Errorf("AWS Secrets Manager에서 키 로드 실패: %w", err)
	}

	return parsePEMToRSAKey(*result.SecretString)
}

func parsePEMToRSAKey(pemStr string) (*rsa.PrivateKey, error) {
	// 1. PEM 문자열을 DER 바이트로 디코딩
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, fmt.Errorf("PEM 디코딩 실패: 유효한 PEM 블록 없음")
	}

	// 2. DER 바이트를 RSA 구조체로 파싱
	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// PKCS8 실패하면 PKCS1도 시도
		// openssl 버전에 따라 포맷이 다를 수 있음
		rsaKey, err2 := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("키 파싱 실패 PKCS8: %w, PKCS1: %w", err, err2)
		}
		privateKey = rsaKey
		// return rsaKey, nil ??
	}

	// 3. interface{} → *rsa.PrivateKey 타입 단언
	rsaKey, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("RSA 키가 아님 (EC나 다른 타입)")
	}

	return rsaKey, nil
}

func createJWT(user_id int64, event_id string) (string, error) {

	now := time.Now()

	claims := jwt.MapClaims{
		"sub":      user_id,                         // Subject
		"iss":      tokenIssuer,                     // Issuer
		"aud":      jwt.ClaimStrings{tokenAudience}, // Audience
		"iat":      now.Unix(),                      // Issued At
		"exp":      now.Add(TOKEN_DURATION).Unix(),  // Expiration Time
		"jti":      generateJTI(),                   // JWT ID (토큰 고유 ID. 블랙리스트로 중복방지)
		"user_id":  user_id,
		"event_id": event_id,

		// "roles": []string{"USER"},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = tokenKeyID // JWKS에서 키 찾을 때 사용

	return token.SignedString(jwtSecret)
}

func RunWaitingQueueWorkerAll(ctx context.Context, wqRepository *queue.WaitingQueueRepository, sseHub *sse.Hub) {
	var wg sync.WaitGroup

	wqRepository.Snapshots.Range(func(event_id string, snapshot *queue.WaitingQueueSnapshot) bool {
		wg.Go(func() {
			waitingQueueWorker(ctx, event_id, wqRepository, sseHub)
		})
		return true
	})

	wg.Wait()
}

func waitingQueueWorker(ctx context.Context, event_id string, wqRepository *queue.WaitingQueueRepository, sseHub *sse.Hub) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	slog.Info("waitingQueueWorker start", "event_id", event_id)

	for {
		select {
		case <-ctx.Done():
			slog.Error("waitingQueueWorker done", "event_id", event_id)
			return

		case <-ticker.C:
			// 1분마다 스냅샷 업데이트
			err := wqRepository.UpdateSnapshot(ctx, event_id)
			if err != nil {
				slog.Error("error update snapshot", "error", err, "event_id", event_id)
			}

		default:
			// 0순위 유저를 꺼내서 토큰 발급
			user_id, err := wqRepository.Pop(ctx, event_id)
			if err != nil {
				if err == queue.ErrQueueEmpty {
					time.Sleep(1 * time.Second)
					continue
				}

				slog.Error("error pop from waiting room", "error", err, "event_id", event_id)
				continue
			}
			token, err := createJWT(user_id, event_id)
			if err != nil {
				slog.Error("error creating JWT", "error", err, "event_id", event_id, "user_id", user_id)
				continue
			}

			slog.Info("waitingQueueWorker pop user", "event_id", event_id, "user_id", user_id, "token", token)

			// SSE로 토큰 전달
			exist := sseHub.SendToken(ctx, user_id, event_id, token)
			if !exist {
				slog.Info("token not sent", "event_id", event_id, "user_id", user_id)
				// 토큰을 전달할 클라이언트가 없는 경우, Broadcast로 모든 클라이언트에게 토큰 전달
				if err := mq.BroadcastToken(ctx, user_id, event_id, token); err != nil {
					slog.Error("error sending user event to API", "error", err, "event_id", event_id, "user_id", user_id)
				}
			}
			// 그 결과 브라우저는 SSE 종료, 토큰들고 java  예매페이지로 리다이렉트
		}
	}
}

func HandleJWKS(c *echo.Context) error {
	publicKey := &jwtSecret.PublicKey

	// RSA public key를 JWK 형식으로 변환
	nBytes := publicKey.N.Bytes()
	eBytes := big.NewInt(int64(publicKey.E)).Bytes()

	jwk := types.JWK{
		Kty: "RSA",
		Use: "sig",
		Kid: tokenKeyID,
		Alg: "RS256",
		N:   base64.RawURLEncoding.EncodeToString(nBytes),
		E:   base64.RawURLEncoding.EncodeToString(eBytes),
	}

	return c.JSON(http.StatusOK, types.JWKSResponse{Keys: []types.JWK{jwk}})
}

func generateJTI() string {
	b := make([]byte, 16)
	rand.Read(b)                // 16바이트 랜덤 데이터 생성 (128비트)
	return fmt.Sprintf("%x", b) // 16진수 문자열로 변환 → 32자리
}
