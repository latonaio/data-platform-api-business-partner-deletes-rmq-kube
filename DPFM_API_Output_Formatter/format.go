package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToGeneral(rows *sql.Rows) (*General, error) {
	defer rows.Close()
	general := General{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&general.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &general, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &general, nil
	}

	return &general, nil
}

func ConvertToRole(rows *sql.Rows) (*[]Role, error) {
	defer rows.Close()
	roles := make([]Role, 0)
	i := 0

	for rows.Next() {
		i++
		role := Role{}
		err := rows.Scan(
			&role.BusinessPartner,
			&role.BusinessPartnerRole,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &roles, err
		}

		roles = append(roles, role)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &roles, nil
	}

	return &roles, nil
}

func ConvertToPerson(rows *sql.Rows) (*[]Person, error) {
	defer rows.Close()
	persons := make([]Person, 0)
	i := 0

	for rows.Next() {
		i++
		person := Person{}
		err := rows.Scan(
			&person.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &persons, err
		}

		persons = append(persons, person)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &persons, nil
	}

	return &persons, nil
}

func ConvertToSNS(rows *sql.Rows) (*[]SNS, error) {
	defer rows.Close()
	sNSs := make([]SNS, 0)
	i := 0

	for rows.Next() {
		i++
		sNS := SNS{}
		err := rows.Scan(
			&sNS.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &sNSs, err
		}

		sNSs = append(sNSs, sNS)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &sNSs, nil
	}

	return &sNSs, nil
}

func ConvertToGPS(rows *sql.Rows) (*[]GPS, error) {
	defer rows.Close()
	gPSs := make([]GPS, 0)
	i := 0

	for rows.Next() {
		i++
		gPS := GPS{}
		err := rows.Scan(
			&gPS.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &gPSs, err
		}

		gPSs = append(gPSs, gPS)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &gPSs, nil
	}

	return &gPSs, nil
}

func ConvertToRank(rows *sql.Rows) (*[]Rank, error) {
	defer rows.Close()
	ranks := make([]Rank, 0)
	i := 0

	for rows.Next() {
		i++
		rank := Rank{}
		err := rows.Scan(
			&rank.BusinessPartner,
			&rank.RankType,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &ranks, err
		}

		ranks = append(ranks, rank)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &ranks, nil
	}

	return &ranks, nil
}

func ConvertToPersonOrganization(rows *sql.Rows) (*[]PersonOrganization, error) {
	defer rows.Close()
	personOrganizations := make([]PersonOrganization, 0)
	i := 0

	for rows.Next() {
		i++
		personOrganization := PersonOrganization{}
		err := rows.Scan(
			&personOrganization.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &personOrganizations, err
		}

		personOrganizations = append(personOrganizations, personOrganization)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &personOrganizations, nil
	}

	return &personOrganizations, nil
}

func ConvertToPersonMobilePhoneAuth(rows *sql.Rows) (*[]PersonMobilePhoneAuth, error) {
	defer rows.Close()
	personMobilePhoneAuths := make([]PersonMobilePhoneAuth, 0)
	i := 0

	for rows.Next() {
		i++
		personMobilePhoneAuth := PersonMobilePhoneAuth{}
		err := rows.Scan(
			&personMobilePhoneAuth.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &personMobilePhoneAuths, err
		}

		personMobilePhoneAuths = append(personMobilePhoneAuths, personMobilePhoneAuth)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &personMobilePhoneAuths, nil
	}

	return &personMobilePhoneAuths, nil
}

func ConvertToPersonGoogleAccountAuth(rows *sql.Rows) (*[]PersonGoogleAccountAuth, error) {
	defer rows.Close()
	personGoogleAccountAuths := make([]PersonGoogleAccountAuth, 0)
	i := 0

	for rows.Next() {
		i++
		personGoogleAccountAuth := PersonGoogleAccountAuth{}
		err := rows.Scan(
			&personGoogleAccountAuth.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &personGoogleAccountAuths, err
		}

		personGoogleAccountAuths = append(personGoogleAccountAuths, personGoogleAccountAuth)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &personGoogleAccountAuths, nil
	}

	return &personGoogleAccountAuths, nil
}

func ConvertToFinInst(rows *sql.Rows) (*[]FinInst, error) {
	defer rows.Close()
	finInsts := make([]FinInst, 0)
	i := 0

	for rows.Next() {
		i++
		finInst := FinInst{}
		err := rows.Scan(
			&finInst.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &finInsts, err
		}

		finInsts = append(finInsts, finInst)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &finInsts, nil
	}

	return &finInsts, nil
}

func ConvertToAccounting(rows *sql.Rows) (*[]Accounting, error) {
	defer rows.Close()
	accountings := make([]Accounting, 0)
	i := 0

	for rows.Next() {
		i++
		accounting := Accounting{}
		err := rows.Scan(
			&accounting.BusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &accountings, err
		}

		accountings = append(accountings, accounting)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &accountings, nil
	}

	return &accountings, nil
}
