package controllers

import (
	"encoding/json"
	"log"
	"net/http"
	service "notifications/internal/pkg/service"
	"time"
    
	"github.com/gorilla/mux"
)

// Constants
const (
	contentTypeHeader     = "application/json"
	internalErrorCode     = "INTERNAL_ERROR"
	notFoundErrorCode     = "NOT_FOUND"
	invalidRequestCode    = "INVALID_REQUEST"
	internalErrorMessage  = "Failed to retrieve document"
	notFoundErrorMessage  = "Document not found"
	invalidRequestMessage = "Invalid JSON body"
	missingIndexMessage   = "Index parameter is required"
	encodingErrorMessage  = "Failed to encode response"
)

// Response Structs
type getDocByIDResponse struct {
	Data interface{} `json:"data"`
}

type getDocByTextResponse struct {
	Data interface{} `json:"data"`
}

type getAllDocsResponse struct {
	Data interface{} `json:"data"`
}

type searchByTimeRangeResponse struct {
	Data interface{} `json:"data"`
}

// Controller
type Elastic_search_Controller struct {
	elasticsearchService *service.Elasticsearch_Service
}

func New_ElasticSearch_Controller(esService *service.Elasticsearch_Service) *Elastic_search_Controller {
	return &Elastic_search_Controller{elasticsearchService: esService}
}
   
// Handler functions
func (h *Elastic_search_Controller) Get_Doc_By_ID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentTypeHeader)

	vars := mux.Vars(r)
	index := "sms_index"
	id := vars["id"]
	log.Printf("getDocByID: Received request for document with ID %s from index %s", id, index)

	doc, err := h.elasticsearchService.Get_Document_By_ID(index, id)
	if err != nil {
		handleInternalError(w, err, "getDocByID")
		return
	}

	if doc == nil {
		handleNotFound(w, id, index, "getDocByID")
		return
	}

	response := getDocByIDResponse{Data: doc}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError(w, err, "getDocByID")
	}
}

func (h *Elastic_search_Controller) Get_Doc_By_Text(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentTypeHeader)
    
	vars := mux.Vars(r)
	index := "sms_index"
	text := vars["text"]
	log.Printf("getDocByText: Received request for text '%s' from index %s", text, index)

	docs, err := h.elasticsearchService.Search_By_Text(index, text)
	if err != nil {
		handleInternalError(w, err, "getDocByText")
		return
	}

	if len(docs) == 0 {
		handleNotFound(w, text, index, "getDocByText")
		return
	}
    
	response := getDocByTextResponse{Data: docs}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError(w, err, "getDocByText")
	}
}

func (h *Elastic_search_Controller) Get_All_Docs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentTypeHeader)
 
	var requestBody struct {
		Index string `json:"index"`
	}

	// Decode the request body
	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		log.Printf("getAllDocs: Error decoding request body: %v", err)
		http.Error(w, `{"error":{"code":"INVALID_REQUEST","message":"Invalid JSON body"}}`, http.StatusBadRequest)
		return
	}

	index := requestBody.Index
	if index == "" {
		http.Error(w, `{"error":{"code":"INVALID_REQUEST","message":"Index parameter is required"}}`, http.StatusBadRequest)
		return
	}

	docs, err := h.elasticsearchService.Get_All_Documents(index)
	if err != nil {
		log.Printf("getAllDocs: Error retrieving documents from index %s: %v", index, err)
		http.Error(w, `{"error":{"code":"INTERNAL_ERROR","message":"Failed to retrieve documents"}}`, http.StatusInternalServerError)
		return
	}

	response := getAllDocsResponse{Data: docs}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError(w, err, "getAllDocs")
	}
}
   
func (h *Elastic_search_Controller) Get_Doc_By_TimeRange(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentTypeHeader)

	var request struct {
		Index     string    `json:"index"`
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
	}
	// Decode request body
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		log.Printf("getDocByTimeRange: Invalid input to search by time: %v", err)
		http.Error(w, `{"error":{"code":"INVALID_REQUEST","message":"Invalid input"}}`, http.StatusBadRequest)
		return
	}

	docs, err := h.elasticsearchService.Search_By_TimeRange(request.Index, request.StartTime, request.EndTime)
	if err != nil {
		log.Printf("getDocByTimeRange: Error retrieving documents from index %s between %s and %s: %v", request.Index, request.StartTime, request.EndTime, err)
		http.Error(w, `{"error":{"code":"INTERNAL_ERROR","message":"Failed to retrieve documents"}}`, http.StatusInternalServerError)
		return
	}

	if len(docs) == 0 {
		log.Printf("getDocByTimeRange: No documents found in index %s between %s and %s", request.Index, request.StartTime, request.EndTime)
		http.Error(w, `{"error":{"code":"NOT_FOUND","message":"No documents found"}}`, http.StatusNotFound)
		return
	}

	response := searchByTimeRangeResponse{Data: docs}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError(w, err, "getDocByTimeRange")
	}
}
      
// Helper functions
func handleInternalError(w http.ResponseWriter, err error, method string) {
	log.Printf("%s: Error: %v", method, err)
	http.Error(w, `{"error":{"code":"INTERNAL_ERROR","message":"Failed to retrieve document"}}`, http.StatusInternalServerError)
}
   
func handleNotFound(w http.ResponseWriter, id string, index string, method string) {
	log.Printf("%s: Document with ID %s not found in index %s", method, id, index)
	http.Error(w, `{"error":{"code":"NOT_FOUND","message":"Document not found"}}`, http.StatusNotFound)
}
  
func handleEncodingError(w http.ResponseWriter, err error, method string) {
	log.Printf("%s: Error encoding response: %v", method, err)
	http.Error(w, `{"error":{"code":"INTERNAL_ERROR","message":"Failed to encode response"}}`, http.StatusInternalServerError)
}
  