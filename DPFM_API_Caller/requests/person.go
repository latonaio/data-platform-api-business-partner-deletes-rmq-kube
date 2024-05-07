package requests

type Person struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
