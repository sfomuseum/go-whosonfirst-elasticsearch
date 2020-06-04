package client

import (
	es "github.com/elastic/go-elasticsearch"
)

func NewClientWithEndpoint(es_endpoint string) (*es.Client, error) {

	es_cfg := es.Config{
		Addresses: []string{es_endpoint},
	}

	return es.NewClient(es_cfg)
}
