package requests

type User struct {
	UserID				string	`json:"UserID"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
