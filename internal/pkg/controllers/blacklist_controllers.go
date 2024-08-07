// package controllers

// import (
// 	"encoding/json"
// 	"log"
// 	"net/http"
// 	"notifications/internal/pkg/service"

// 	"github.com/gorilla/mux"
// )

// // Define constants for response messages
// const (
// 	BlacklistedStatus         = "blacklisted"
// 	NotBlacklistedStatus      = "not blacklisted"
// 	NumberNotFoundError       = "Number does not exist"
// 	NumberNotBlacklistedError = "Number is not blacklisted"
// 	InternalError             = "Internal error occurred"
// 	InvalidRequestError       = "Invalid request"
// 	SuccessMessage            = "Successfully removed from blacklist"
// )

// // Define response structs
// type ErrorResponse_Blacklist struct {
// 	Error struct {
// 		Code    string `json:"code"`
// 		Message string `json:"message"`
// 	} `json:"error"`
// }

// type StatusResponse struct {
// 	Data struct {
// 		Number string `json:"number"`
// 		Status string `json:"status"`
// 	} `json:"data"`
// }

// type BlackList_Controller struct {
// 	blacklistService *service.Blacklist_Service
// }

// func New_Blacklist_Controller(blacklistService *service.Blacklist_Service) *BlackList_Controller {
// 	return &BlackList_Controller{blacklistService: blacklistService}
// }

// type Success_Response struct {
// 	Data string `json:"data"`
// }

// type Success_Response_List struct {
// 	Data []string `json:"data"`
// }

// func (h *BlackList_Controller) Get_All_From_BlackList(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")

// 	ctx := r.Context()
// 	blacklist, err := h.blacklistService.Get_All_Blacklisted_Numbers(ctx)
// 	if err != nil {
// 		log.Printf("Error retrieving blacklist: %v", err)
// 		http.Error(w, encodeJSON(ErrorResponse_Blacklist{
// 			Error: struct {
// 				Code    string `json:"code"`
// 				Message string `json:"message"`
// 			}{
// 				Code:    "INTERNAL_ERROR",
// 				Message: InternalError,
// 			},
// 		}), http.StatusInternalServerError)
// 		return
// 	}

// 	response := Success_Response_List{
// 		Data: blacklist,
// 	}
// 	if err := json.NewEncoder(w).Encode(response); err != nil {
// 		log.Printf("Error encoding blacklist response: %v", err)
// 	}
// }

// func (h *BlackList_Controller) Add_Number_To_BlackList(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	var request struct {
// 		Numbers []string `json:"numbers"`
// 	}
// 	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
// 		log.Printf("Invalid input in AddBlackList: %v", err)
// 		http.Error(w, encodeJSON(ErrorResponse_Blacklist{
// 			Error: struct {
// 				Code    string `json:"code"`
// 				Message string `json:"message"`
// 			}{
// 				Code:    "INVALID_REQUEST",
// 				Message: InvalidRequestError,
// 			},
// 		}), http.StatusBadRequest)
// 		return
// 	}

// 	success, already, err := h.blacklistService.Add_To_Blacklist(request.Numbers)
// 	if err != nil {
// 		log.Printf("Error adding numbers to blacklist: %v", err)
// 		http.Error(w, encodeJSON(ErrorResponse_Blacklist{
// 			Error: struct {
// 				Code    string `json:"code"`
// 				Message string `json:"message"`
// 			}{
// 				Code:    "INTERNAL_ERROR",
// 				Message: InternalError,
// 			},
// 		}), http.StatusInternalServerError)
// 		return
// 	}

// 	response := map[string]interface{}{
// 		"success": success,
// 		"already": already,
// 	}
// 	if err := json.NewEncoder(w).Encode(response); err != nil {
// 		log.Printf("Error encoding AddBlackList response: %v", err)
// 	}
// }

// func (h *BlackList_Controller) Delete_From_BlackList(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	vars := mux.Vars(r)
// 	number := vars["number"]

// 	err := h.blacklistService.Remove_From_Blacklist(number)
// 	if err != nil {
// 		var statusCode int
// 		var errorResponse ErrorResponse_Blacklist

// 		if err.Error() == NumberNotFoundError {
// 			log.Printf("DeleteBlackList: Number not found: %s", number)
// 			statusCode = http.StatusNotFound
// 			errorResponse = ErrorResponse_Blacklist{
// 				Error: struct {
// 					Code    string `json:"code"`
// 					Message string `json:"message"`
// 				}{
// 					Code:    "INVALID_REQUEST",
// 					Message: NumberNotFoundError,
// 				},
// 			}
// 		} else if err.Error() == NumberNotBlacklistedError {
// 			log.Printf("DeleteBlackList: Number not blacklisted: %s", number)
// 			statusCode = http.StatusNotFound
// 			errorResponse = ErrorResponse_Blacklist{
// 				Error: struct {
// 					Code    string `json:"code"`
// 					Message string `json:"message"`
// 				}{
// 					Code:    "INVALID_REQUEST",
// 					Message: NumberNotBlacklistedError,
// 				},
// 			}
// 		} else {
// 			log.Printf("DeleteBlackList: Failed to remove number from blacklist: %v", err)
// 			statusCode = http.StatusInternalServerError
// 			errorResponse = ErrorResponse_Blacklist{
// 				Error: struct {
// 					Code    string `json:"code"`
// 					Message string `json:"message"`
// 				}{
// 					Code:    "INTERNAL_ERROR",
// 					Message: InternalError,
// 				},
// 			}
// 		}
// 		http.Error(w, encodeJSON(errorResponse), statusCode)
// 		return
// 	}

// 	response := Success_Response{
// 		Data: SuccessMessage,
// 	}
// 	if err := json.NewEncoder(w).Encode(response); err != nil {
// 		log.Printf("Error encoding DeleteBlackList response: %v", err)
// 	}
// }

// func (h *BlackList_Controller) Get_BlackList_By_ID(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")
// 	vars := mux.Vars(r)
// 	number := vars["number"]
// 	ctx := r.Context()
// 	isBlacklisted, err := h.blacklistService.Is_Number_Blacklisted(ctx, number)
// 	if err != nil {
// 		log.Printf("GetBlackListID: Error checking blacklist status for number %s: %v", number, err)
// 		http.Error(w, encodeJSON(ErrorResponse_Blacklist{
// 			Error: struct {
// 				Code    string `json:"code"`
// 				Message string `json:"message"`
// 			}{
// 				Code:    "INTERNAL_ERROR",
// 				Message: InternalError,
// 			},
// 		}), http.StatusInternalServerError)
// 		return
// 	}

// 	if !isBlacklisted {
// 		log.Printf("GetBlackListID: Number not blacklisted: %s", number)
// 		http.Error(w, encodeJSON(ErrorResponse_Blacklist{
// 			Error: struct {
// 				Code    string `json:"code"`
// 				Message string `json:"message"`
// 			}{
// 				Code:    "INVALID_REQUEST",
// 				Message: NumberNotBlacklistedError,
// 			},
// 		}), http.StatusNotFound)
// 		return
// 	}

// 	response := StatusResponse{
// 		Data: struct {
// 			Number string `json:"number"`
// 			Status string `json:"status"`
// 		}{
// 			Number: number,
// 			Status: BlacklistedStatus,
// 		},
// 	}
// 	if err := json.NewEncoder(w).Encode(response); err != nil {
// 		log.Printf("Error encoding GetBlackListID response: %v", err)
// 	}
// }

// // Helper function to encode response to JSON
// func encodeJSON(v interface{}) string {
// 	data, err := json.Marshal(v)
// 	if err != nil {
// 		log.Printf("Error marshalling response to JSON: %v", err)
// 		return "{}"
// 	}
// 	return string(data)
// }
package controllers

import (
	"encoding/json"
	"log"
	"net/http"
	"notifications/internal/pkg/service"
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

type BlackList_Controller struct {
	blacklistService *service.Blacklist_Service
}

func New_Blacklist_Controller(blacklistService *service.Blacklist_Service) *BlackList_Controller {
	return &BlackList_Controller{blacklistService: blacklistService}
}

// Handler functions

func (h *BlackList_Controller) Get_All_From_BlackList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	blacklist, err := h.blacklistService.Get_All_Blacklisted_Numbers(ctx)
	if err != nil {
		handleInternalError_Blacklist(w, err, "GetAllFromBlacklist")
		return
	}

	response := SuccessResponseList{
		Data: blacklist,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError_Blacklist(w, err, "GetAllFromBlacklist")
	}
}

func (h *BlackList_Controller) Add_Number_To_BlackList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var request struct {
		Numbers []string `json:"numbers"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		handleInvalidRequest(w, err, "AddBlacklistNumbers")
		return
	}

	success, already, err := h.blacklistService.Add_To_Blacklist(request.Numbers)
	if err != nil {
		handleInternalError_Blacklist(w, err, "AddBlacklistNumbers")
		return
	}

	response := map[string]interface{}{
		"success": success,
		"already": already,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError_Blacklist(w, err, "AddBlacklistNumbers")
	}
}

func (h *BlackList_Controller) Delete_From_BlackList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	number := vars["number"]

	err := h.blacklistService.Remove_From_Blacklist(number)
	if err != nil {
		handleBlacklistRemovalError(w, err, number)
		return
	}

	response := SuccessResponse{
		Data: SuccessMessage,
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		handleEncodingError_Blacklist(w, err, "DeleteFromBlacklist")
	}
}

func (h *BlackList_Controller) Get_BlackList_By_ID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)
	number := vars["number"]
	ctx := r.Context()

	isBlacklisted, err := h.blacklistService.Is_Number_Blacklisted(ctx, number)
	if err != nil {
		handleInternalError_Blacklist(w, err, "GetBlacklistByID")
		return
	}

	if !isBlacklisted {
		handleNotFound_Blacklist(w, number, "GetBlacklistByID", NumberNotBlacklistedError)
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
		handleEncodingError_Blacklist(w, err, "GetBlacklistByID")
	}
}

// Helper functions
func handleInternalError_Blacklist(w http.ResponseWriter, err error, method string) {
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

func handleNotFound_Blacklist(w http.ResponseWriter, number string, method string, message string) {
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

func handleEncodingError_Blacklist(w http.ResponseWriter, err error, method string) {
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
