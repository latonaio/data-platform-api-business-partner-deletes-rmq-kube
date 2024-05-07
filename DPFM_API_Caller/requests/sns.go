package requests

type SNS struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
