package requests

type PersoneInstagramAuth struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
