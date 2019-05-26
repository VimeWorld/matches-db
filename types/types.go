package types

const SnowflakeEpoch uint64 = 1546300800000

type UserMatch struct {
	Id  uint64 `json:"id"`
	Win bool   `json:"win"`
}

func GetSnowflakeTs(id uint64) uint64 {
	return (id >> 22) + SnowflakeEpoch
}
