package transports

type HttpTransport struct {
	OpenLineageURL    string
	OpenLineageAPIKey string
}

func (transport *HttpTransport) Send(lineageEvent string) error {
	return nil
}
