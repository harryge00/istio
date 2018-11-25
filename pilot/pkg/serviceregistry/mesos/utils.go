package mesos

import (
	"io/ioutil"
	"time"
	"net/http"
	"encoding/json"
	"istio.io/istio/pilot/pkg/model"
)

type DNSRecord struct {
	Host     string `json:"host,omitempty"`
	Name     model.Hostname `json:"name,omitempty"`
	RType     string `json:"rtype,omitempty"`
}

type dnsClient struct {
	// address for dcos-dns
	baseURL string

	client http.Client
}

func newDNSClient(url string, timeout time.Duration) *dnsClient {
	client := http.Client{
		Timeout: timeout,
	}

	return &dnsClient{
		baseURL: url,
		client: client,
	}
}

func (dc *dnsClient) getAllRecords() (records []DNSRecord, err error) {
	resp, err := dc.client.Get(dc.baseURL + "/v1/records")
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

func convertServices(record *DNSRecord) *model.Service {
	return &model.Service{
		Hostname:     record.Name,
		Address:      record.Host,
	}
}

func (dc *dnsClient) getHost(host string) (records []DNSRecord, err error) {
	resp, err := dc.client.Get(dc.baseURL + "/v1/hosts/" + host)
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