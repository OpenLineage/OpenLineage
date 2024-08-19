package transport

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/pretty"
)

type ConsoleConfig struct {
	PrettyPrint bool
}

type consoleTransport struct {
	prettyPrint bool
}

func (ct *consoleTransport) Emit(ctx context.Context, event any) error {
	body, err := json.Marshal(&event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if ct.prettyPrint {
		body = pretty.Pretty(body)
	}

	if _, err := fmt.Println(string(body)); err != nil {
		return fmt.Errorf("emit event to console: %w", err)
	}

	return nil
}
