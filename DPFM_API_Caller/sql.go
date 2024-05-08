package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-authenticator-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-authenticator-deletes-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) UserRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.User {
	where := fmt.Sprintf("WHERE user.UserID = %d ", input.User.UserID)
	rows, err := c.db.Query(
		`SELECT 
			user.UserID
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_authenticator_user_data as user ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToUser(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) SMSAuthsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.SMSAuth {
	where := fmt.Sprintf("WHERE sMSAuth.UserID IS NOT NULL\nAND user.UserID = %d", input.User.UserID)
	rows, err := c.db.Query(
		`SELECT 
			sMSAuth.UserID
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_authenticator_sms_auth_data as user ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToSMSAuth(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) GoogleAccountAuthsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GoogleAccountAuth {
	where := fmt.Sprintf("WHERE googleAccountAuth.UserID IS NOT NULL\nAND user.UserID = %d", input.User.UserID)
	rows, err := c.db.Query(
		`SELECT 
			googleAccountAuth.UserID
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_authenticator_google_account_auth_data as user ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGoogleAccountAuth(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
