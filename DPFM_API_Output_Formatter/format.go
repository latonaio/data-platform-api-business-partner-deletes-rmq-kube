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
