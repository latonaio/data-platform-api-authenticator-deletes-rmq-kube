package requests

type GoogleAccountAuth struct {
	UserID				string	`json:"UserID"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
