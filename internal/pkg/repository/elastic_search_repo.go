package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"notifications/internal/models"
	"strings"
	"time"
	"notifications/configurations"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ElasticsearchRepo struct {
	client *elasticsearch.Client
}

func NewElasticSearch(addr string) (*ElasticsearchRepo, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{addr},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("NewElasticsearchRepo: Failed to create Elasticsearch client: %v", err)
		return nil, err
	}
	return &ElasticsearchRepo{client: client}, nil
}
func (r *ElasticsearchRepo) CreateIndex(index string) error {
	// Check if the index exists and delete it
	if exists, err := r.IndexExists(index); err != nil {
		return err
	} else if exists {
		if err := r.DeleteIndex(index); err != nil {
			return err
		}
	}

	// Define the mapping with date formats
	mapping := `{
		"mappings": {
			"properties": {
				"created_at": {
					"type": "date",
					"format": "strict_date_time"
				},
				"updated_at": {
					"type": "date",
					"format": "strict_date_time"
				}
			}
		}
	}`

	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(mapping),
	}

	res, err := req.Do(context.Background(), r.client)
	if err != nil {
		log.Printf("CreateIndex: Failed to create index %s: %v", index, err)
		return fmt.Errorf("failed to create index %s: %w", index, err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("CreateIndex: Elasticsearch error: %s", res.String())
		return fmt.Errorf("elasticsearch error: %s", res.String())
	}

	log.Printf("CreateIndex: Index %s created successfully", index)
	return nil
}

// Helper function to check if an index exists
func (r *ElasticsearchRepo) IndexExists(index string) (bool, error) {
	res, err := r.client.Indices.Exists([]string{index})
	if err != nil {
		return false, fmt.Errorf("failed to check if index %s exists: %w", index, err)
	}
	defer res.Body.Close()

	// Index exists if the status code is 200 OK
	return res.StatusCode == 200, nil
}

// Helper function to delete an index
func (r *ElasticsearchRepo) DeleteIndex(index string) error {
	req := esapi.IndicesDeleteRequest{
		Index: []string{index},
	}

	res, err := req.Do(context.Background(), r.client)
	if err != nil {
		log.Printf("DeleteIndex: Failed to delete index %s: %v", index, err)
		return fmt.Errorf("failed to delete index %s: %w", index, err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Printf("DeleteIndex: Elasticsearch error: %s", res.String())
		return fmt.Errorf("elasticsearch error: %s", res.String())
	}

	log.Printf("DeleteIndex: Index %s deleted successfully", index)
	return nil
}

func (r *ElasticsearchRepo) IndexDocument(index string, id string, doc interface{}) error {
	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		log.Printf("IndexDocument: Failed to encode document: %v", err)
		return fmt.Errorf("failed to encode document: %w", err)
	}

	// Convert strings.Builder to strings.Reader
	reader := strings.NewReader(buf.String())

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       reader,
		Refresh:    "true",
	}

	res, err := req.Do(context.Background(), r.client)
	if err != nil {
		log.Printf("IndexDocument: Failed to index document: %v", err)
		return fmt.Errorf("failed to index document: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("IndexDocument: Elasticsearch error: %s", res.String())
		return fmt.Errorf("elasticsearch error: %s", res.String())
	}

	log.Printf("IndexDocument: Document indexed successfully, ID: %s", id)
	return nil
}
func (r *ElasticsearchRepo) GetDocument(index string, id string) (map[string]interface{}, error) {
	// Perform the GET request to Elasticsearch
	res, err := r.client.Get(index, id)
	if err != nil {
		log.Printf("GetDocument: Failed to retrieve document ID %s: %v", id, err)
		return nil, fmt.Errorf("failed to retrieve document ID %s: %w", id, err)
	}
	defer res.Body.Close()

	// Check if the response indicates an error
	if res.IsError() {
		log.Printf("GetDocument: Elasticsearch error: %s", res.String())
		return nil, fmt.Errorf("elasticsearch error: %s", res.String())
	}

	// Decode the response body into a map
	var response map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		log.Printf("GetDocument: Failed to decode response body for document ID %s: %v", id, err)
		return nil, fmt.Errorf("failed to decode response body for document ID %s: %w", id, err)
	}

	// Extract the document from the _source field
	source, ok := response["_source"].(map[string]interface{})
	if !ok {
		log.Printf("GetDocument: Document ID %s not found in response", id)
		return nil, fmt.Errorf("document ID %s not found in response", id)
	}

	log.Printf("GetDocument: Document retrieved successfully, ID: %s", id)
	return source, nil
}

func (r *ElasticsearchRepo) DeleteDocument(index string, id string) error {
	req := esapi.DeleteRequest{
		Index:      index,
		DocumentID: id,
	}

	res, err := req.Do(context.Background(), r.client)
	if err != nil {
		log.Printf("DeleteDocument: Failed to delete document ID %s: %v", id, err)
		return fmt.Errorf("failed to delete document ID %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("DeleteDocument: Elasticsearch error: %s", res.String())
		return fmt.Errorf("elasticsearch error: %s", res.String())
	}

	log.Printf("DeleteDocument: Document deleted successfully, ID: %s", id)
	return nil
}

func (e *ElasticsearchRepo) CreateIndexSMS(sms models.SMS, dur time.Duration) error {
	// Format the dates
	
	updatedAt := time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	createdAt := time.Now().UTC().Add(5*time.Hour + 30*time.Minute - dur)
	// Prepare the document
	doc := map[string]interface{}{
		"id":               sms.ID,
		"phone_number":     sms.PhoneNumber,
		"message":          sms.Message,
		"status":           sms.Status,
		"failure_comments": sms.FailureComments,
		"created_at":       createdAt,
		"updated_at":       updatedAt,
	}

	// Convert the document to JSON
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("error marshaling document to JSON: %w", err)
	}
	// Index the document in Elasticsearch
	_, err = e.client.Index(
		"sms_index", // Index name
		bytes.NewReader(body),
		e.client.Index.WithDocumentID(sms.ID),
		e.client.Index.WithRefresh("true"), // Ensures the index is refreshed after the operation
	)
	if err != nil {
		return fmt.Errorf("error indexing document in Elasticsearch: %w", err)
	}

	return nil
}

// In repository/elasticsearchrepo.go
func (r *ElasticsearchRepo) Search(index string, query string) (*esapi.Response, error) {
	res, err := r.client.Search(
		r.client.Search.WithContext(context.Background()),
		r.client.Search.WithIndex(index),
		r.client.Search.WithBody(strings.NewReader(query)),
		r.client.Search.WithSize(10000), // Adjust size as needed
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search documents in index %s: %w", index, err)
	}
	return res, nil
}
func GetElasticRepo()(*ElasticsearchRepo,error){
	elasticRepo, err := NewElasticSearch(config.ElasticsearchAddr)
	if err != nil {
		return nil, err
	}
	if err := elasticRepo.CreateIndex("sms_index"); err != nil {
		return nil, err
	}
	return elasticRepo,nil
}
