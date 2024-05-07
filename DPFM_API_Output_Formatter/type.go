package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	General					*[]General		`json:"General"`
	Role					*[]Role			`json:"Role"`
	Person					*[]Person		`json:"Person"`
	SNS                     *[]SNS			`json:"SNS"`
	GPS                     *[]GPS			`json:"GPS"`
	Rank                    *[]Rank			`json:"Rank"`
	PersonOrganization      *[]PersonOrganization      `json:"PersonOrganization"`
	PersonMobilePhoneAuth	*[]PersonMobilePhoneAuth	`json:"PersonMobilePhoneAuth"`
	PersonGoogleAccountAuth	*[]PersonGoogleAccountAuth	`json:"PersonGoogleAccountAuth"`
	PersonInstagramAuth		*[]PersonInstagramAuth		`json:"PersonInstagramAuth"`
	FinInst					*[]FinInst		`json:"FinInst"`
	Accounting				*[]Accounting	`json:"Accounting"`
}

type General struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type Role struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	BusinessPartnerRole	string	`json:"BusinessPartnerRole"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type Person struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type SNS struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type GPS struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type Rank struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	RankType			string	`json:"RankType"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type PersonOrganization struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type PersonMobilePhoneAuth struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type PersonGoogleAccountAuth struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type PersonInstagramAuth struct {
	BusinessPartner     int		`json:"BusinessPartner"`
	IsMarkedForDeletion *bool	`json:"IsMarkedForDeletion"`
}

type FinInst struct {
	BusinessPartner     	int		`json:"BusinessPartner"`
	FinInstIdentification	int     `json:"FinInstIdentification"`
	IsMarkedForDeletion		*bool	`json:"IsMarkedForDeletion"`
}

type Accounting struct {
	BusinessPartner     int   `json:"BusinessPartner"`
	IsMarkedForDeletion *bool `json:"IsMarkedForDeletion"`
}
