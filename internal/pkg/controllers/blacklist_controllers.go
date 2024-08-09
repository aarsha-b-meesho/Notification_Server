package controllers

import (
	"encoding/json"
	"log"
	"net/http"
	service "notifications/internal/pkg/service"

	"github.com/gorilla/mux"
)

// Define constants for response messages
const (
	BlacklistedStatus         = "blacklisted"
	NotBlacklistedStatus      = "not blacklisted"
	NumberNotFoundError       = "Number does not exist"
	NumberNotBlacklistedError = "Number is not blacklisted"
	InternalError             = "Internal error occurred"
	InvalidRequestError       = "Invalid request"
	SuccessMessage            = "Successfully removed from blacklist"
)

// Define response structs
type ErrorResponseBlacklist struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type StatusResponse struct {
	Data struct {
		Number string `json:"number"`
		Status string `json:"status"`
	} `json:"data"`
}

type SuccessResponse struct {
	Data string `json:"data"`
}

type SuccessResponseList struct {
	Data []string `json:"data"`
}

type BlackListController struct {
	blacklistService *service.BlacklistService
}


// Handler functions
func GetBlackListController()*BlackListController{
	blacklistService := service.GetNewBlackListSerevice()
	return &BlackListController{blacklistService: blacklistService}
}
func (h *BlackListController) GetAllFromBlackList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	blacklist, err := h.blacklistService.GetAllFromBlacklist(ctx)
	if err != nil {
		handleinternalErrorBlacklist(w, err, "GetAllFromBlacklist")
		return
	}

	response := SuccessResponseList{
		Data: blacklist,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingErrorBlacklist(w, err, "GetAllFromBlacklist")
	}
}

func (h *BlackListController) AddNumberToBlacklist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var request struct {
		Numbers []string `json:"numbers"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		handleInvalidRequest(w, err, "AddBlacklistNumbers")
		return
	}

	success, already, err := h.blacklistService.AddToBlacklist(request.Numbers)
	if err != nil {
		handleinternalErrorBlacklist(w, err, "AddBlacklistNumbers")
		return
	}

	response := map[string]interface{}{
		"success": success,
		"already": already,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingErrorBlacklist(w, err, "AddBlacklistNumbers")
	}
}

func (h *BlackListController) DeleteNumberFromBlacklist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	number := vars["number"]

	err := h.blacklistService.RemoveFromBlacklist(number)
	if err != nil {
		handleBlacklistRemovalError(w, err, number)
		return
	}

	response := SuccessResponse{
		Data: SuccessMessage,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingErrorBlacklist(w, err, "DeleteFromBlacklist")
	}
}

func (h *BlackListController) GetBlacklistByID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	number := vars["number"]
	ctx := r.Context()

	isBlacklisted, err := h.blacklistService.IsNumberBlacklisted(ctx, number)
	if err != nil {
		handleinternalErrorBlacklist(w, err, "GetBlacklistByID")
		return
	}

	if !isBlacklisted {
		handleNotFoundBlacklist(w, number, "GetBlacklistByID", NumberNotBlacklistedError)
		return
	}

	response := StatusResponse{
		Data: struct {
			Number string `json:"number"`
			Status string `json:"status"`
		}{
			Number: number,
			Status: BlacklistedStatus,
		},
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingErrorBlacklist(w, err, "GetBlacklistByID")
	}
}

// Helper functions
func handleinternalErrorBlacklist(w http.ResponseWriter, err error, method string) {
	log.Printf("%s: Error: %v", method, err)
	http.Error(w, encodeJSON(ErrorResponseBlacklist{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{
			Code:    "INTERNAL_ERROR",
			Message: InternalError,
		},
	}), http.StatusInternalServerError)
}

func handleInvalidRequest(w http.ResponseWriter, err error, method string) {
	log.Printf("%s: Invalid input: %v", method, err)
	http.Error(w, encodeJSON(ErrorResponseBlacklist{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{
			Code:    "INVALID_REQUEST",
			Message: InvalidRequestError,
		},
	}), http.StatusBadRequest)
}

func handleBlacklistRemovalError(w http.ResponseWriter, err error, number string) {
	var statusCode int
	var errorResponse ErrorResponseBlacklist

	switch err.Error() {
	case NumberNotFoundError:
		log.Printf("DeleteFromBlacklist: Number not found: %s", number)
		statusCode = http.StatusNotFound
		errorResponse = ErrorResponseBlacklist{
			Error: struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{
				Code:    "INVALID_REQUEST",
				Message: NumberNotFoundError,
			},
		}
	case NumberNotBlacklistedError:
		log.Printf("DeleteFromBlacklist: Number not blacklisted: %s", number)
		statusCode = http.StatusNotFound
		errorResponse = ErrorResponseBlacklist{
			Error: struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{
				Code:    "INVALID_REQUEST",
				Message: NumberNotBlacklistedError,
			},
		}
	default:
		log.Printf("DeleteFromBlacklist: Failed to remove number from blacklist: %v", err)
		statusCode = http.StatusInternalServerError
		errorResponse = ErrorResponseBlacklist{
			Error: struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}{
				Code:    "INTERNAL_ERROR",
				Message: InternalError,
			},
		}
	}
	http.Error(w, encodeJSON(errorResponse), statusCode)
}

func handleNotFoundBlacklist(w http.ResponseWriter, number string, method string, message string) {
	log.Printf("%s: Number not blacklisted: %s", method, number)
	http.Error(w, encodeJSON(ErrorResponseBlacklist{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{
			Code:    "INVALID_REQUEST",
			Message: message,
		},
	}), http.StatusNotFound)
}

func handleEncodingErrorBlacklist(w http.ResponseWriter, err error, method string) {
	log.Printf("%s: Error encoding response: %v", method, err)
	http.Error(w, encodeJSON(ErrorResponseBlacklist{
		Error: struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{
			Code:    "INTERNAL_ERROR",
			Message: InternalError,
		},
	}), http.StatusInternalServerError)
}

func encodeJSON(v interface{}) string {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("Error marshalling response to JSON: %v", err)
		return "{}"
	}
	return string(data)
}
