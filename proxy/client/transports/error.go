package transports

type TransportError struct {
	StatusCode int
	message    string
}

func (e *TransportError) Error() string {
	return e.message
}
