package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"strings"
)

func (c *DPFMAPICaller) GeneralRead(
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

func (c *DPFMAPICaller) AccountingsRead(
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
