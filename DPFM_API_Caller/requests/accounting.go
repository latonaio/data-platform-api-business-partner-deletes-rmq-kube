package requests

type Accounting struct {
	BusinessPartner         int     `json:"BusinessPartner"`
	IsMarkedForDeletion		*bool   `json:"IsMarkedForDeletion"`
}
