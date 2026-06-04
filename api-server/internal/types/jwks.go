package types

type JWKSResponse struct {
	Keys []JWK `json:"keys"`
}

// RFC 7517
type JWK struct {
	Kty string `json:"kty"` // 키 타입: RSA
	Use string `json:"use"` // 용도: sig (서명 검증)
	Kid string `json:"kid"` // 키 ID
	Alg string `json:"alg"` // 알고리즘: RS256

	// RFC 7518
	N string `json:"n"` // RSA modulus (base64url)
	E string `json:"e"` // RSA exponent (base64url)
}
