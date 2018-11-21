package mesos

import (
	"io/ioutil"
	"time"
	"net/http"
	"encoding/json"
)

type DNSRecord struct {
	Host     string `json:"host,omitempty"`
	Name     string `json:"name,omitempty"`
	RType     string `json:"rtype,omitempty"`
}

type dnsClient struct {
	dnsURL string
	client http.Client
}

func newDNSClient(url string, timeout time.Duration) *dnsClient {
	client := http.Client{
		Timeout: timeout,
	}

	return &dnsClient{
		dnsURL: url,
		client: client,
	}
}

func (dc *dnsClient) getAllRecords() (err error, records []DNSRecord) {
	resp, err := dc.client.Get(dc.dnsURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &records)
	return
}