package api

type RequestSave struct {
	MatchId uint64 `json:"matchid"`
	Users   []struct {
		Id  uint32 `json:"id"`
		Win bool   `json:"win"`
	} `json:"users"`
}
