package requests

type PersonMobilePhoneAuth struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
