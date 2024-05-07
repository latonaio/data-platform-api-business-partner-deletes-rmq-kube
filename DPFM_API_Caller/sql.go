package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"strings"
)

func (c *DPFMAPICaller) GeneralDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.General {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE general.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	general.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGeneral(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) RoleDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Role {
	where := fmt.Sprintf("WHERE general.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	where := fmt.Sprintf("WHERE role.BusinessPartnerRole IS NOT NULL\nAND role.BusinessPartnerRole =  \"%s\"", input.Role.BusinessPartnerRole)
	rows, err := c.db.Query(
		`SELECT 
			role.BusinessPartner, role.BusinessPartnerRole
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_role_data as role
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = role.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToRole(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) RolesDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Role {
	where := fmt.Sprintf("WHERE role.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			role.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_role_data as role
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = role.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToRole(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) PersonDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Person {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE person.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")
	rows, err := c.db.Query(
		`SELECT 
			person.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_person_data as person
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPerson(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) RankDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Rank {
	where := fmt.Sprintf("WHERE general.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	where := fmt.Sprintf("WHERE rank.RankType IS NOT NULL\nAND rank.RankType =  \"%s\"", input.Rank.RankType)
	rows, err := c.db.Query(
		`SELECT 
			rank.BusinessPartner, rank.RankType
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_rank_data as rank
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = rank.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToRank(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) RanksDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Rank {
	where := fmt.Sprintf("WHERE rank.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			rank.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_rank_data as rank
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = rank.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToRank(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) PersonMobilePhoneAuthDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonMobilePhoneAuth {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE personMobilePhoneAuth.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")
	rows, err := c.db.Query(
		`SELECT 
			personMobilePhoneAuth.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_person_mobile_phone_auth_data as personMobilePhoneAuth
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPersonMobilePhoneAuth(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) PersonGoogleAccountAuthDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonGoogleAccountAuth {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE personGoogleAccountAuth.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")
	rows, err := c.db.Query(
		`SELECT 
			personGoogleAccountAuth.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_person_google_account_auth_data as personGoogleAccountAuth
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPersonGoogleAccountAuth(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) PersonInstagramAuthDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonInstagramAuth {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE personInstagramAuth.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")
	rows, err := c.db.Query(
		`SELECT 
			personInstagramAuth.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_person_instagram_auth_data as personInstagramAuth
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPersonInstagramAuth(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) FinInstDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.FinInst {
	where := fmt.Sprintf("WHERE general.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	where := fmt.Sprintf("WHERE finInst.FinInstIdentification IS NOT NULL\nAND finInst.FinInstIdentification =  %d", input.FinInst.FinInstIdentification)
	rows, err := c.db.Query(
		`SELECT 
			finInst.BusinessPartner, finInst.FinInstIdentification
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_finInst_data as finInst
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = finInst.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToFinInst(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) FinInstsDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.FinInst {
	where := fmt.Sprintf("WHERE finInst.BusinessPartner IS NOT NULL\nAND general.BusinessPartner = %d", input.General.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			finInst.BusinessPartner, finInst.FinInstIdentification
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_finInst_data as finInst
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_general_data as general
		ON general.BusinessPartner = finInst.BusinessPartner ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToFinInst(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AccountingDelete(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Accounting {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE accounting.BusinessPartner = %d ", input.General.BusinessPartner),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	accounting.BusinessPartner
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_business_partner_accounting_data as accounting 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAccounting(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
