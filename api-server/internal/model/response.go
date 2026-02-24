package model

type EnterResponse struct {
	Sequence int64  `json:"seq"`
	Token    string `json:"token"` // auth token
}
