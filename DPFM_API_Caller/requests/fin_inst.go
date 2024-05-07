package requests

type FinInst struct {
	BusinessPartner     	int		`json:"BusinessPartner"`
	FinInstIdentification	int     `json:"FinInstIdentification"`
	IsMarkedForDeletion		*bool	`json:"IsMarkedForDeletion"`
}
