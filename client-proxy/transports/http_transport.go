package transports

type HttpTransport struct {
	OpenLineageURL    string
	OpenLineageAPIKey string
}

func (transport *HttpTransport) Emit(lineageEvent string) error {
	return nil
}
