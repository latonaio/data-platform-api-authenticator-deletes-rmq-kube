package requests

type InstagramAuth struct {
	UserID				string	`json:"UserID"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
