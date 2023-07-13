package requests

type General struct {
	BusinessPartner         int		`json:"BusinessPartner"`
	IsMarkedForDeletion		*bool   `json:"IsMarkedForDeletion"`
}
