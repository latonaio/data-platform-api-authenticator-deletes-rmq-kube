package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToUser(rows *sql.Rows) (*User, error) {
	defer rows.Close()
	user := User{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&user.UserID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &user, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return nil, nil
	}

	return &user, nil
}

func ConvertToSMSAuth(rows *sql.Rows) (*[]SMSAuth, error) {
	defer rows.Close()
	sMSAuths := make([]SMSAuth, 0)
	i := 0

	for rows.Next() {
		i++
		sMSAuth := SMSAuth{}
		err := rows.Scan(
			&sMSAuth.UserID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &sMSAuths, err
		}

		sMSAuths = append(sMSAuths, sMSAuth)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &sMSAuths, nil
	}

	return &sMSAuths, nil
}

func ConvertToGoogleAccountAuth(rows *sql.Rows) (*[]GoogleAccountAuth, error) {
	defer rows.Close()
	googleAccountAuths := make([]GoogleAccountAuth, 0)
	i := 0

	for rows.Next() {
		i++
		googleAccountAuth := GoogleAccountAuth{}
		err := rows.Scan(
			&googleAccountAuth.UserID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &googleAccountAuths, err
		}

		googleAccountAuths = append(googleAccountAuths, googleAccountAuth)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &googleAccountAuths, nil
	}

	return &googleAccountAuths, nil
}

func ConvertToInstagramAuth(rows *sql.Rows) (*[]InstagramAuth, error) {
	defer rows.Close()
	instagramAuths := make([]InstagramAuth, 0)
	i := 0

	for rows.Next() {
		i++
		instagramAuth := InstagramAuth{}
		err := rows.Scan(
			&instagramAuth.UserID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &instagramAuths, err
		}

		instagramAuths = append(instagramAuths, instagramAuth)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &instagramAuths, nil
	}

	return &instagramAuths, nil
}
