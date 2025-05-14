package kvstore

type Op struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}
