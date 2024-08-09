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

// MessageController handles requests related to messages
type MessageController struct {
	MessageService *service.MessageService
}

// NewMessageController creates a new MessageController
func NewMessageController() *MessageController {
	message:= service.GetMessageService()
	return &MessageController{MessageService: message}
}
func GetMessageController() *MessageController{
	return NewMessageController()
}
// NotifyServer handles requests to notify the server
func (h *MessageController) NotifyServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	var sms models.SMS
	if err := json.NewDecoder(r.Body).Decode(&sms); err != nil {
		h.sendErrorResponseMessage(w, ErrorInvalidInput, http.StatusBadRequest)
		return
	}
    
	h.setSMSFields(&sms)
    
	if err := h.MessageService.CreateMessage(&sms); err != nil {
		log.Printf("NotifyServer: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToInsertIntoDB, http.StatusInternalServerError)
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

// SendMessageToUsers handles requests to send messages
func (h *MessageController) SendMessageToUsers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	results, err := h.MessageService.ProcessMessages()
	if err != nil {
		log.Printf("SendMessageToUsers: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToProcessMessages, http.StatusInternalServerError)
		return
	}
    
	response := SendMessageResponse{
		Response: results,
	}
	h.sendSuccessResponse(w, response)
}

// GetAllMessages handles requests to get all messages
func (h *MessageController) GetAllMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)
    
	smsList, err := h.MessageService.GetAllMessages()
	if err != nil {
		log.Printf("GetAllMessages: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToRetrieveSMSList, http.StatusInternalServerError)
		return
	}
    
	response := GetAllMessagesResponse{
		Data: smsList,
	}
	h.sendSuccessResponse(w, response)
}

// GetMessageByID handles requests to get a message by ID
func (h *MessageController) GetMessageByID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", ContentTypeJSON)

	vars := mux.Vars(r)
	msgID := vars["ID"]

	// Check if the ID exists
	exists, err := h.MessageService.CheckIDExists(msgID)
	if err != nil {
		log.Printf("GetMessageByID: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToCheckIDExists, http.StatusInternalServerError)
		return
	}

	if !exists {
		h.sendErrorResponseMessage(w, ErrorIDNotFound, http.StatusNotFound)
		return
	}

	// Retrieve the SMS message by ID
	sms, err := h.MessageService.GetMessageByID(msgID)
	if err != nil {
		log.Printf("GetMessageByID: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToRetrieveSMS, http.StatusInternalServerError)
		return
	}

	// Create response data
	response := GetMessageByIDResponse{
		Data: sms, 
	}

	// Send success response
	sms.FailureCode = "200"
	h.sendSuccessResponse(w, response)
}


// Helper function to set SMS fields
func (h *MessageController) setSMSFields(sms *models.SMS) {
	rand.Seed(uint64(time.Now().UnixNano()))
	sms.ID = fmt.Sprintf("%v", rand.Intn(9999999)+1)
	sms.CreatedAt = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	sms.UpdatedAt = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
}

// Helper function to send success responses
func (h *MessageController) sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("sendSuccessResponse: %v", err)
		h.sendErrorResponseMessage(w, ErrorFailedToEncodeResponse, http.StatusInternalServerError)
	}
}

// Helper function to send error responses
func (h *MessageController) sendErrorResponseMessage(w http.ResponseWriter, errorMessage string, statusCode int) {
	http.Error(w, errorMessage, statusCode)
}
