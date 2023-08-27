package core

type Order struct {
	ID                 string  `db:"id" json:"id"`
	Status             string  `db:"status" json:"status"`
	DestinationAddress string  `db:"destination_address" json:"destinationAddress"`
	Value              float64 `db:"value" json:"value"`
	CreatedAt          string  `db:"created_at" json:"createdAt"`
}
