package controllers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"notifications/internal/models"
	service "notifications/internal/pkg/service"
	"time"

	"github.com/gorilla/mux"
	"golang.org/x/exp/rand"
)

// Define constants for error messages and status codes
const (
	ContentTypeJSON               = "application/json"
	ErrorInvalidInput             = `{"error":{"code":"INVALID_REQUEST","message":"Invalid input"}}`
	ErrorInternalServer           = `{"error":{"code":"INTERNAL_ERROR","message":"Internal server error"}}`
	ErrorFailedToInsertIntoDB     = `{"error":{"code":"INTERNAL_ERROR","message":"Failed to insert into database"}}`
	ErrorFailedToProcessMessages  = `{"error":{"code":"INTERNAL_ERROR","message":"Error processing messages"}}`
	ErrorFailedToRetrieveSMSList  = `{"error":{"code":"INTERNAL_ERROR","message":"Error retrieving SMS list"}}`
	ErrorFailedToCheckIDExists    = `{"error":{"code":"INTERNAL_ERROR","message":"Failed to check ID existence"}}`
	ErrorIDNotFound               = `{"error":{"code":"INVALID_REQUEST","message":"request_ID not found"}}`
	ErrorFailedToRetrieveSMS      = `{"error":{"code":"INTERNAL_ERROR","message":"Failed to retrieve SMS details"}}`
	ErrorFailedToEncodeResponse   = `{"error":{"code":"INTERNAL_SERVER_ERROR","message":"Unable to encode response"}}`
)

// Response structs for different methods
type NotifyServerResponse struct {
	RequestID string    `json:"requestID"`
	Comments  string    `json:"comments"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type SendMessageResponse struct {
	Response interface{} `json:"response"`
}

type GetAllMessagesResponse struct {
	Data []models.SMS `json:"data"`
}

type GetMessageByIDResponse struct {
	Data *models.SMS `json:"data"`
}

type ErrorResponse_Message struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Message_Controller handles requests related to messages
type Message_Controller struct {
	Message_Service *service.Message_Service
}

// New_Message_Controller creates a new Message_Controller
func New_Message_Controller(messageService *service.Message_Service) *Message_Controller {
	return &Message_Controller{Message_Service: messageService}
}

// Notify_Server handles requests to notify the server
func (h *Message_Controller) Notify_Server(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	var sms models.SMS
	if err := json.NewDecoder(r.Body).Decode(&sms); err != nil {
		h.sendErrorResponse_Message(w, ErrorInvalidInput, http.StatusBadRequest)
		return
	}
    
	h.setSMSFields(&sms)
    
	if err := h.Message_Service.Create_SMS(&sms); err != nil {
		log.Printf("Notify_Server: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToInsertIntoDB, http.StatusInternalServerError)
		return
	}
    
	response := NotifyServerResponse{
		RequestID: sms.ID,
		Comments:  "Successfully inserted into DB and produced in Kafka",
		CreatedAt: sms.CreatedAt,
		UpdatedAt: sms.UpdatedAt,
	}
	h.sendSuccessResponse(w, response)
}

// Send_Message handles requests to send messages
func (h *Message_Controller) Send_Message(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	results, err := h.Message_Service.Process_Messages()
	if err != nil {
		log.Printf("Send_Message: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToProcessMessages, http.StatusInternalServerError)
		return
	}
    
	response := SendMessageResponse{
		Response: results,
	}
	h.sendSuccessResponse(w, response)
}

// Get_All_Messages handles requests to get all messages
func (h *Message_Controller) Get_All_Messages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	smsList, err := h.Message_Service.Get_All_Messages()
	if err != nil {
		log.Printf("Get_All_Messages: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToRetrieveSMSList, http.StatusInternalServerError)
		return
	}
    
	response := GetAllMessagesResponse{
		Data: smsList,
	}
	h.sendSuccessResponse(w, response)
}

// Get_Message_By_Id handles requests to get a message by ID
func (h *Message_Controller) Get_Message_By_Id(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)

	vars := mux.Vars(r)
	msgID := vars["ID"]

	// Check if the ID exists
	exists, err := h.Message_Service.Check_ID_Exists(msgID)
	if err != nil {
		log.Printf("Get_Message_By_Id: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToCheckIDExists, http.StatusInternalServerError)
		return
	}

	if !exists {
		h.sendErrorResponse_Message(w, ErrorIDNotFound, http.StatusNotFound)
		return
	}

	// Retrieve the SMS message by ID
	sms, err := h.Message_Service.Get_Message_By_ID(msgID)
	if err != nil {
		log.Printf("Get_Message_By_Id: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToRetrieveSMS, http.StatusInternalServerError)
		return
	}

	// Create response data
	response := GetMessageByIDResponse{
		Data: sms, // Use sms directly, assuming sms is of type models.SMS
	}

	// Send success response
	h.sendSuccessResponse(w, response)
}


// Helper function to set SMS fields
func (h *Message_Controller) setSMSFields(sms *models.SMS) {
	rand.Seed(uint64(time.Now().UnixNano()))
	sms.ID = fmt.Sprintf("%v", rand.Intn(9999999)+1)
	sms.CreatedAt = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	sms.UpdatedAt = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
}

// Helper function to send success responses
func (h *Message_Controller) sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("sendSuccessResponse: %v", err)
		h.sendErrorResponse_Message(w, ErrorFailedToEncodeResponse, http.StatusInternalServerError)
	}
}

// Helper function to send error responses
func (h *Message_Controller) sendErrorResponse_Message(w http.ResponseWriter, errorMessage string, statusCode int) {
	http.Error(w, errorMessage, statusCode)
}
