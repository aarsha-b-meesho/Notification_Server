package service

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"notifications/internal/pkg/repository"
	"time"
)

// Constants
const (
	DateFormat = "strict_date_optional_time"
)

// Error Messages
const (
	ErrElasticsearchSearch   = "elasticsearch search error"
	ErrFailedToDecodeResults = "failed to decode search results"
	ErrEmptyDocumentID       = "document ID cannot be empty"
)

type Elasticsearch_Service struct {
	repo *repository.ElasticsearchRepo
}

func New_ElasticSearch_Service(repo *repository.ElasticsearchRepo) *Elasticsearch_Service {
	return &Elasticsearch_Service{repo: repo}
}

func (e *Elasticsearch_Service) Get_All_Documents(index string) ([]map[string]interface{}, error) {
	query := `{
		"query": {
			"match_all": {}
		}
	}`

	res, err := e.repo.Search(index, query)
	if err != nil {
		return nil, e.Handle_Error("GetAllDocuments", err, index)
	}
	defer res.Body.Close()

	searchResults, err := e.Decode_Search_Results(res.Body)
	if err != nil {
		return nil, err
	}

	var allDocs []map[string]interface{}
	for _, hit := range searchResults.Hits.Hits {
		allDocs = append(allDocs, hit.Source)
	}

	log.Printf("GetAllDocuments: Retrieved %d documents from index %s", len(allDocs), index)
	return allDocs, nil
}

func (e *Elasticsearch_Service) Get_Document_By_ID(index string, id string) (map[string]interface{}, error) {
	if id == "" {
		return nil, fmt.Errorf(ErrEmptyDocumentID)
	}

	query := fmt.Sprintf(`{
		"query": {
			"ids": {
				"values": ["%s"]
			}
		}
	}`, id)

	res, err := e.repo.Search(index, query)
	if err != nil {
		return nil, e.Handle_Error("GetDocumentByID", err, index)
	}
	defer res.Body.Close()

	searchResults, err := e.Decode_Search_Results(res.Body)
	if err != nil {
		return nil, err
	}

	if len(searchResults.Hits.Hits) == 0 {
		log.Printf("GetDocumentByID: Document ID %s not found in index %s", id, index)
		return nil, nil
	}

	doc := searchResults.Hits.Hits[0].Source
	log.Printf("GetDocumentByID: Document retrieved successfully, ID: %s", id)
	return doc, nil
}

func (e *Elasticsearch_Service) Search_By_Text(index string, text string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`{
		"query": {
			"match": {
				"message": {
					"query": "%s",
					"operator": "and"
				}
			}
		}
	}`, text)

	res, err := e.repo.Search(index, query)
	if err != nil {
		return nil, e.Handle_Error("SearchByText", err, index)
	}
	defer res.Body.Close()

	searchResults, err := e.Decode_Search_Results(res.Body)
	if err != nil {
		return nil, err
	}

	var matchingDocs []map[string]interface{}
	for _, hit := range searchResults.Hits.Hits {
		matchingDocs = append(matchingDocs, hit.Source)
	}

	log.Printf("SearchByText: Retrieved %d documents containing text '%s' from index %s", len(matchingDocs), text, index)
	return matchingDocs, nil
}

func (e *Elasticsearch_Service) Search_By_TimeRange(index string, startTime time.Time, endTime time.Time) ([]map[string]interface{}, error) {
	startTimeStr := startTime.Format(time.RFC3339)
	endTimeStr := endTime.Format(time.RFC3339)

	query := fmt.Sprintf(`{
		"query": {
			"range": {
				"created_at": {
					"gte": "%s",
					"lte": "%s",
					"format": "strict_date_optional_time"
				}
			}
		}
	}`, startTimeStr, endTimeStr)

	res, err := e.repo.Search(index, query)
	if err != nil {
		return nil, e.Handle_Error("SearchByTimeRange", err, index)
	}
	defer res.Body.Close()

	searchResults, err := e.Decode_Search_Results(res.Body)
	if err != nil {
		return nil, err
	}

	var docsInRange []map[string]interface{}
	for _, hit := range searchResults.Hits.Hits {
		docsInRange = append(docsInRange, hit.Source)
	}

	log.Printf("SearchByTimeRange: Retrieved %d documents from index %s between %s and %s", len(docsInRange), index, startTimeStr, endTimeStr)
	return docsInRange, nil
}

// Handle_Error handles Elasticsearch errors and logs them
func (e *Elasticsearch_Service) Handle_Error(methodName string, err error, index string) error {
	log.Printf("%s: %s in index %s: %v", methodName, ErrElasticsearchSearch, index, err)
	return fmt.Errorf("%s: %w", ErrElasticsearchSearch, err)
}

// Decode_Search_Results decodes the Elasticsearch search results
func (e *Elasticsearch_Service) Decode_Search_Results(body io.Reader) (struct {
	Hits struct {
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}, error) {
	var searchResults struct {
		Hits struct {
			Hits []struct {
				Source map[string]interface{} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(body).Decode(&searchResults); err != nil {
		return searchResults, fmt.Errorf(ErrFailedToDecodeResults+": %w", err)
	}
	return searchResults, nil
}
