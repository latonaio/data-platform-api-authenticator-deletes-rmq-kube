package requests

type SMSAuth struct {
	UserID				string	`json:"UserID"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
