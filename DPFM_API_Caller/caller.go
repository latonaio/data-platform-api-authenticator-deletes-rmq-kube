package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-authenticator-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-authenticator-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-authenticator-deletes-rmq-kube/config"

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
	switch input.APIType {
	case "deletes":
		response = c.deleteSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var userData *dpfm_api_output_formatter.User
	sMSAuthData := make([]dpfm_api_output_formatter.SMSAuth, 0)
	googleAccountAuthData := make([]dpfm_api_output_formatter.GoogleAccountAuth, 0)
	instagramAuthData := make([]dpfm_api_output_formatter.InstagramAuth, 0)
	for _, a := range accepter {
		switch a {
		case "User":
			h, i, s, t := c.userDelete(input, output, log)
			userData = h
			if h == nil || i == nil || s == nil || t == nil{
				continue
			}
			sMSAuthData = append(sMSAuthData, *i...)
			googleAccountAuthData = append(googleAccountAuthData, *s...)
			instagramAuthData = append(instagramAuthData, *t...)
		case "SMSAuth":
			i := c.sMSAuthDelete(input, output, log)
			if i == nil {
				continue
			}
			sMSAuthData = append(sMSAuthData, *i...)
		case "GoogleAccountAuth":
			s := c.googleAccountAuthDelete(input, output, log)
			if s == nil {
				continue
			}
			googleAccountAuthData = append(googleAccountAuthData, *s...)
		}
		case "InstagramAuth":
			t := c.instagramAuthDelete(input, output, log)
			if t == nil {
				continue
			}
			instagramAuthData = append(instagramAuthData, *t...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		User:				userData,
		SMSAuth:			&sMSAuthData,
		GoogleAccountAuth:	&googleAccountAuthData,
		InstagramAuth:		&instagramAuthData,
	}
}

func (c *DPFMAPICaller) userDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.User, *[]dpfm_api_output_formatter.SMSAuth, *[]dpfm_api_output_formatter.GoogleAccountAuth, *[]dpfm_api_output_formatter.InstagramAuth) {
	sessionID := input.RuntimeSessionID

	user := c.UserRead(input, log)
	if user == nil {
		return nil, nil, nil, nil
	}
	user.IsMarkedForDeletion = input.User.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": user, "function": "AuthenticatorUser", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "User Data cannot delete"
		return nil, nil, nil, nil
	}
	// userの削除フラグが取り消された時は子に影響を与えない
	if !*user.IsMarkedForDeletion {
		return user, nil, nil, nil
	}

	sMSAuths := c.SMSAuthsRead(input, log)
	for i := range *sMSAuths {
		(*sMSAuths)[i].IsMarkedForDeletion = input.User.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*sMSAuths)[i], "function": "AuthenticatorSMSAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "SMSAuth Data cannot delete"
			return nil, nil, nil, nil
		}
	}

	googleAccountAuths := c.GoogleAccountAuthsRead(input, log)
	for i := range *googleAccountAuths {
		(*googleAccountAuths)[i].IsMarkedForDeletion = input.User.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*googleAccountAuths)[i], "function": "AuthenticatorGoogleAccountAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "GoogleAccountAuth Data cannot delete"
			return nil, nil, nil, nil
		}
	}

	instagramAuths := c.InstagramAuthsRead(input, log)
	for i := range *instagramAuths {
		(*instagramAuths)[i].IsMarkedForDeletion = input.User.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*instagramAuths)[i], "function": "AuthenticatorInstagramAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "InstagramAuth Data cannot delete"
			return nil, nil, nil, nil
		}
	}
	
	return user, sMSAuths, googleAccountAuths, instagramAuths
}

func (c *DPFMAPICaller) sMSAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.SMSAuth) {
	sessionID := input.RuntimeSessionID
	sMSAuth := input.User.SMSAuth[0]

	sMSAuths := make([]dpfm_api_output_formatter.SMSAuth, 0)
	for _, v := range input.User.SMSAuth {
		data := dpfm_api_output_formatter.SMSAuth{
			UserID:					input.User.UserID,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "AuthenticatorSMSAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "SMSAuth Data cannot delete"
			return nil
		}
	}

	// sMSAuthが削除フラグ取り消しされた場合、userの削除フラグも取り消す
	if !*input.User.SMSAuth[0].IsMarkedForDeletion {
		user := c.UserRead(input, log)
		user.IsMarkedForDeletion = input.User.SMSAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": user, "function": "AuthenticatorUser", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "User Data cannot delete"
			return nil
		}
	}

	return &sMSAuths
}

func (c *DPFMAPICaller) googleAccountAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.GoogleAccountAuth) {
	sessionID := input.RuntimeSessionID
	googleAccountAuth := input.User.GoogleAccountAuth[0]

	googleAccountAuths := make([]dpfm_api_output_formatter.GoogleAccountAuth, 0)
	for _, v := range input.User.GoogleAccountAuth {
		data := dpfm_api_output_formatter.GoogleAccountAuth{
			UserID:					input.User.UserID,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "AuthenticatorGoogleAccountAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "GoogleAccountAuth Data cannot delete"
			return nil
		}
	}

	// googleAccountAuthが削除フラグ取り消しされた場合、userの削除フラグも取り消す
	if !*input.User.GoogleAccountAuth[0].IsMarkedForDeletion {
		user := c.UserRead(input, log)
		user.IsMarkedForDeletion = input.User.GoogleAccountAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": user, "function": "AuthenticatorUser", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "User Data cannot delete"
			return nil
		}
	}

	return &googleAccountAuths
}

func (c *DPFMAPICaller) instagramAuthDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.InstagramAuth) {
	sessionID := input.RuntimeSessionID
	instagramAuth := input.User.InstagramAuth[0]

	instagramAuths := make([]dpfm_api_output_formatter.InstagramAuth, 0)
	for _, v := range input.User.InstagramAuth {
		data := dpfm_api_output_formatter.InstagramAuth{
			UserID:					input.User.UserID,
			IsMarkedForDeletion:	v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "AuthenticatorInstagramAuth", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "InstagramAuth Data cannot delete"
			return nil
		}
	}

	// instagramAuthが削除フラグ取り消しされた場合、userの削除フラグも取り消す
	if !*input.User.InstagramAuth[0].IsMarkedForDeletion {
		user := c.UserRead(input, log)
		user.IsMarkedForDeletion = input.User.InstagramAuth[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": user, "function": "AuthenticatorUser", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "User Data cannot delete"
			return nil
		}
	}

	return &instagramAuths
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
