package requests

type Rank struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	RankType			string	`json:"RankType"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}
