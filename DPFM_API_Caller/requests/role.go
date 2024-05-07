package requests

type Role struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	BusinessPartnerRole	string	`json:"BusinessPartnerRole"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
