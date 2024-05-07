package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-business-partner-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-business-partner-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "deletes" {
		response = c.deleteSqlProcess(input, output, accepter, log)
	}

	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var generalData *dpfm_api_output_formatter.General
	roleData := make([]dpfm_api_output_formatter.Role, 0)
	personData := make([]dpfm_api_output_formatter.Person, 0)
	sNSData := make([]dpfm_api_output_formatter.SNS, 0)
	gPSData := make([]dpfm_api_output_formatter.GPS, 0)
	rankData := make([]dpfm_api_output_formatter.Rank, 0)
	personOrganizationData := make([]dpfm_api_output_formatter.PersonOrganization, 0)
	personMobilePhoneAuthData := make([]dpfm_api_output_formatter.PersonMobilePhoneAuth, 0)
	personGoogleAccountAuthData := make([]dpfm_api_output_formatter.PersonGoogleAccountAuth, 0)
	personInstagramAuthData := make([]dpfm_api_output_formatter.PersonInstagramAuth, 0)
	finInstData := make([]dpfm_api_output_formatter.FinInst, 0)
	accountingData := make([]dpfm_api_output_formatter.Accounting, 0)
	for _, a := range accepter {
		switch a {
		case "General":
			h, i, j, k, m, n, p, q, r, s, t, u := c.generalDelete(input, output, log)
			generalData = h
			if h == nil || i == nil || j == nil || k == nil || m == nil || n == nil || p == nil || q == nil || r == nil || s == nil || t == nil || u == nil{
				continue
			}
			roleData = append(roleData, *i...)
			personData = append(personData, *j...)
			sNSData = append(sNSData, *k...)
			gPSData = append(gPSData, *m...)
			rankData = append(personData, *n...)
			personOrganization = append(personOrganizationData, *p...)
			personMobilePhoneAuthData = append(personMobilePhoneAuthData, *q...)
			personGoogleAccountAuthData = append(personGoogleAccountAuthData, *r...)
			personInstagramAuthData = append(personInstagramAuthData, *s...)
			finInstData = append(finInstData, *t...)
			accountingData = append(accountingData, *u...)
		case "Role":
			i := c.roleDelete(input, output, log)
			if i == nil {
				continue
			}
			roleData = append(roleData, *i...)
		}
		case "Person":
			j := c.personDelete(input, output, log)
			if j == nil {
				continue
			}
			personData = append(personData, *j...)
		}
		case "SNS":
			k := c.sNSDelete(input, output, log)
			if k == nil {
				continue
			}
			sNSData = append(sNSData, *k...)
		}
		case "GPS":
			m := c.gPSDelete(input, output, log)
			if m == nil {
				continue
			}
			gPSData = append(gPSData, *m...)
		}
		case "Rank":
			n := c.rankDelete(input, output, log)
			if n == nil {
				continue
			}
			rankData = append(rankData, *n...)
		}
		case "PersonOrganization":
			p := c.personOrganizationDelete(input, output, log)
			if p == nil {
				continue
			}
			personOrganizationData = append(personOrganizationData, *p...)
		}
		case "PersonMobilePhoneAuth":
			q := c.personMobilePhoneAuthDelete(input, output, log)
			if q == nil {
				continue
			}
			personMobilePhoneAuthData = append(personMobilePhoneAuthData, *q...)
		}
		case "PersonGoogleAccountAuth":
			r := c.personGoogleAccountAuthDelete(input, output, log)
			if r == nil {
				continue
			}
			personGoogleAccountAuthData = append(personGoogleAccountAuthData, *r...)
		}
		case "PersonInstagramAuth":
			s := c.personInstagramAuthDelete(input, output, log)
			if s == nil {
				continue
			}
			personInstagramAuthData = append(personInstagramAuthData, *s...)
		}
		case "FinInst":
			t := c.finInstDelete(input, output, log)
			if t == nil {
				continue
			}
			finInstData = append(finInstData, *t...)
		}
		case "Accounting":
			u := c.accountingDelete(input, output, log)
			if u == nil {
				continue
			}
			accountingData = append(accountingData, *u...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		General:    				generalData,
		Role:						&roleData,
		Person:						&personData,
		SNS:						&sNSData,
		GPS:						&gPSData,
		Rank:						&rankData,
		PersonOrganization:			&personOrganizationData,
		PersonMobilePhoneAuth:		&personMobilePhoneAuthData,
		PersonGoogleAccountAuth:	&personGoogleAccountAuthData,
		PersonInstagramAuth:		&personInstagramAuthData,
		FinInst:					&finInstData,
		Accounting: 				&accountingData,
	}
}

func (c *DPFMAPICaller) generalDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.General, *[]dpfm_api_output_formatter.Role, *[]dpfm_api_output_formatter.Person, *[]dpfm_api_output_formatter.PersonMobilePhoneAuth, *[]dpfm_api_output_formatter.PersonGoogleAccountAuth, *[]dpfm_api_output_formatter.Accounting) {
	sessionID := input.RuntimeSessionID
	general := c.GeneralDelete(input, log)
	general.BusinessPartner = input.General.BusinessPartner
	general.IsMarkedForDeletion = input.General.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "General Data cannot delete"
		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
	}

	// generalの削除が取り消された時は子に影響を与えない
	if !*general.IsMarkedForDeletion {
		return general, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
	}
	
	roles := c.RolesDelete(input, log)
	for i := range *roles {
		(*roles)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*roles)[i], "function": "BusinessPartnerRole", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Role Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}

	person := c.PersonDelete(input, log)
	for i := range *person {
		(*person)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*person)[i], "function": "BusinessPartnerPerson", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Person Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}
	
	sNS := c.SNSDelete(input, log)
	for i := range *sNS {
  		(*sNS)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
  		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*sNS)[i], "function": "BusinessPartnerSNS", "runtime_session_id": sessionID})
  		if err != nil {
    		err = xerrors.Errorf("rmq error: %w", err)
    		log.Error("%+v", err)
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
  		res.Success()
  		if !checkResult(res) {
    		output.SQLUpdateResult = getBoolPtr(false)
    		output.SQLUpdateError = "SNS Data cannot delete"
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
	}
	
	gPS := c.GPSDelete(input, log)
	for i := range *gPS {
  		(*gPS)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
  		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*gPS)[i], "function": "BusinessPartnerGPS", "runtime_session_id": sessionID})
  		if err != nil {
    		err = xerrors.Errorf("rmq error: %w", err)
    		log.Error("%+v", err)
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
  		res.Success()
  		if !checkResult(res) {
    		output.SQLUpdateResult = getBoolPtr(false)
    		output.SQLUpdateError = "GPS Data cannot delete"
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
	}

	rank := c.RankDelete(input, log)
	for i := range *rank {
  		(*rank)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
  		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*rank)[i], "function": "BusinessPartnerRank", "runtime_session_id": sessionID})
  		if err != nil {
    		err = xerrors.Errorf("rmq error: %w", err)
    		log.Error("%+v", err)
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
  		res.Success()
  		if !checkResult(res) {
    		output.SQLUpdateResult = getBoolPtr(false)
    		output.SQLUpdateError = "Rank Data cannot delete"
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
	}
	
	personOrganization := c.PersonOrganizationDelete(input, log)
	for i := range *personOrganization {
  		(*personOrganization)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
  		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*personOrganization)[i], "function": "BusinessPartnerPersonOrganization", "runtime_session_id": sessionID})
  		if err != nil {
    		err = xerrors.Errorf("rmq error: %w", err)
    		log.Error("%+v", err)
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
  		res.Success()
  		if !checkResult(res) {
    		output.SQLUpdateResult = getBoolPtr(false)
    		output.SQLUpdateError = "PersonOrganization Data cannot delete"
    		return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
  		}
	}
	
	personMobilePhoneAuth := c.PersonMobilePhoneAuthDelete(input, log)
	for i := range *personMobilePhoneAuth {
		(*personMobilePhoneAuth)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*personMobilePhoneAuth)[i], "function": "BusinessPartnerPersonMobilePhoneAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonMobilePhoneAuth Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}
	
	personGoogleAccountAuth := c.PersonGoogleAccountAuthDelete(input, log)
	for i := range *personGoogleAccountAuth {
		(*personGoogleAccountAuth)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*personGoogleAccountAuth)[i], "function": "BusinessPartnerPersonGoogleAccountAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonGoogleAccountAuth Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}
	
	finInsts := c.FinInstsDelete(input, log)
	for i := range *finInsts {
		(*finInsts)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*finInsts)[i], "function": "BusinessPartnerFinInst", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "FinInst Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}

	accounting := c.AccountingDelete(input, log)
	for i := range *accounting {
		(*accounting)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*accounting)[i], "function": "BusinessPartnerAccounting", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Accounting Data cannot delete"
			return nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil
		}
	}

	return general, roles, person, sNS, gPS, rank, personOrganization, personMobilePhoneAuth, personGoogleAccountAuth, personInstagramAuth, finInsts, accounting
}

func (c *DPFMAPICaller) roleDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Role {
	sessionID := input.RuntimeSessionID

	role := make([]dpfm_api_output_formatter.Role, 0)
	for _, v := range input.General.Role {
		data := dpfm_api_output_formatter.Role{
			BusinessPartner:		input.General.BusinessPartner,
			BusinessPartnerRole:	v.BusinessPartnerRole,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerRole", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Role Data cannot delete"
			return nil
		}
	}
	// roleが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.Role[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.Role[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &role
}

func (c *DPFMAPICaller) personDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Person {
	sessionID := input.RuntimeSessionID

	person := make([]dpfm_api_output_formatter.Person, 0)
	for _, v := range input.General.Person {
		data := dpfm_api_output_formatter.Person{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerPerson", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Person Data cannot delete"
			return nil
		}
	}
	// personが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.Person[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.Person[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &person
}

func (c *DPFMAPICaller) sNSDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.SNS {
	sessionID := input.RuntimeSessionID

	sNS := make([]dpfm_api_output_formatter.SNS, 0)
	for _, v := range input.General.SNS {
		data := dpfm_api_output_formatter.SNS{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerSNS", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "SNS Data cannot delete"
			return nil
		}
	}
	// sNSが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.SNS[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.SNS[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &sNS
}

func (c *DPFMAPICaller) gPSDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GPS {
	sessionID := input.RuntimeSessionID

	gPS := make([]dpfm_api_output_formatter.GPS, 0)
	for _, v := range input.General.GPS {
		data := dpfm_api_output_formatter.GPS{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerGPS", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "GPS Data cannot delete"
			return nil
		}
	}
	// gPSが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.GPS[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.GPS[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &gPS
}

func (c *DPFMAPICaller) rankDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Rank {
	sessionID := input.RuntimeSessionID

	rank := make([]dpfm_api_output_formatter.Rank, 0)
	for _, v := range input.General.Rank {
		data := dpfm_api_output_formatter.Rank{
			BusinessPartner:		input.General.BusinessPartner,
			RankType:				v.RankType,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerRank", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Rank Data cannot delete"
			return nil
		}
	}
	// rankが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.Rank[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.Rank[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &rank
}

func (c *DPFMAPICaller) personOrganizationDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonOrganization {
	sessionID := input.RuntimeSessionID

	personOrganization := make([]dpfm_api_output_formatter.PersonOrganization, 0)
	for _, v := range input.General.PersonOrganization {
		data := dpfm_api_output_formatter.PersonOrganization{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerPersonOrganization", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonOrganization Data cannot delete"
			return nil
		}
	}
	// personOrganizationが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.PersonOrganization[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.PersonOrganization[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &personOrganization
}

func (c *DPFMAPICaller) personMobilePhoneAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonMobilePhoneAuth {
	sessionID := input.RuntimeSessionID

	personMobilePhoneAuth := make([]dpfm_api_output_formatter.PersonMobilePhoneAuth, 0)
	for _, v := range input.General.PersonMobilePhoneAuth {
		data := dpfm_api_output_formatter.PersonMobilePhoneAuth{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerPersonMobilePhoneAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonMobilePhoneAuth Data cannot delete"
			return nil
		}
	}
	// personMobilePhoneAuthが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.PersonMobilePhoneAuth[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.PersonMobilePhoneAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &personMobilePhoneAuth
}

func (c *DPFMAPICaller) personGoogleAccountAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonGoogleAccountAuth {
	sessionID := input.RuntimeSessionID

	personGoogleAccountAuth := make([]dpfm_api_output_formatter.PersonGoogleAccountAuth, 0)
	for _, v := range input.General.PersonGoogleAccountAuth {
		data := dpfm_api_output_formatter.PersonGoogleAccountAuth{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerPersonGoogleAccountAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonGoogleAccountAuth Data cannot delete"
			return nil
		}
	}
	// personGoogleAccountAuthが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.PersonGoogleAccountAuth[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.PersonGoogleAccountAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &personGoogleAccountAuth
}

func (c *DPFMAPICaller) personInstagramAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.PersonInstagramAuth {
	sessionID := input.RuntimeSessionID

	personInstagramAuth := make([]dpfm_api_output_formatter.PersonInstagramAuth, 0)
	for _, v := range input.General.PersonInstagramAuth {
		data := dpfm_api_output_formatter.PersonInstagramAuth{
			BusinessPartner:		input.General.BusinessPartner,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerPersonInstagramAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "PersonInstagramAuth Data cannot delete"
			return nil
		}
	}
	// personInstagramAuthが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.PersonInstagramAuth[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.PersonInstagramAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &personInstagramAuth
}

func (c *DPFMAPICaller) finInstDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.FinInst {
	sessionID := input.RuntimeSessionID

	finInst := make([]dpfm_api_output_formatter.FinInst, 0)
	for _, v := range input.General.FinInst {
		data := dpfm_api_output_formatter.FinInst{
			BusinessPartner:		input.General.BusinessPartner,
			FinInstIdentification:	v.FinInstIdentification,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerFinInst", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "FinInst Data cannot delete"
			return nil
		}
	}
	// finInstが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.FinInst[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.FinInst[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &finInst
}

func (c *DPFMAPICaller) accountingDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Accounting {
	sessionID := input.RuntimeSessionID

	accounting := make([]dpfm_api_output_formatter.Accounting, 0)
	for _, v := range input.General.Accounting {
		data := dpfm_api_output_formatter.Accounting{
			BusinessPartner:     input.General.BusinessPartner,
			IsMarkedForDeletion: v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "BusinessPartnerAccounting", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Accounting Data cannot delete"
			return nil
		}
	}
	// accountingが削除取り消しされた場合、generalの削除も取り消す
	if !*input.General.Accounting[0].IsMarkedForDeletion {
		general := c.GeneralDelete(input, log)
		general.IsMarkedForDeletion = input.General.Accounting[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "BusinessPartnerGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &accounting
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
