package initations

import (
	"fmt"
	"log"
	"net/http"
	"os"

	controllers "notifications/internal/pkg/controllers"

	"github.com/gorilla/mux"
)

// InitializeControllers initializes the controllers and returns them.
func InitializeControllers() (
	*controllers.MessageController,
	*controllers.BlackListController,
	*controllers.ElasticSearchController,
) {
	messageController := controllers.GetMessageController()
	blacklistController := controllers.GetBlackListController()
	elasticSearchController := controllers.GetElasticController()

	return messageController, blacklistController, elasticSearchController
}

// SetupRouter sets up the HTTP routes and starts the server.
func SetupRouter(
	MessageController *controllers.MessageController,
	BlackListController *controllers.BlackListController,
	ElasticsearchController *controllers.ElasticSearchController,
) {
	r := mux.NewRouter()

	// Define routes
	r.HandleFunc("/blacklist", BlackListController.AddNumberToBlacklist).Methods("POST")
	r.HandleFunc("/blacklist", BlackListController.GetAllFromBlackList).Methods("GET")
	r.HandleFunc("/blacklist/{number}", BlackListController.DeleteNumberFromBlacklist).Methods("DELETE")
	r.HandleFunc("/blacklist/{number}", BlackListController.GetBlacklistByID).Methods("GET")
	r.HandleFunc("/sms/{ID}", MessageController.GetMessageByID).Methods("GET")
	r.HandleFunc("/sms", MessageController.NotifyServer).Methods("POST")
	r.HandleFunc("/sms", MessageController.GetAllMessages).Methods("GET")
	r.HandleFunc("/notify", MessageController.SendMessageToUsers).Methods("GET")
	r.HandleFunc("/elastic/{id}", ElasticsearchController.GetDocByID).Methods("GET")
	r.HandleFunc("/elastic", ElasticsearchController.GetAllDocs).Methods("GET")
	r.HandleFunc("/elastictext/{text}", ElasticsearchController.GetDocByText).Methods("GET")
	r.HandleFunc("/elasticsearchbytime", ElasticsearchController.GetDocsByTimeRange).Methods("GET")
	fmt.Println("Starting the server at :8000")
	log.Println("Starting the server at :8000")

	err := http.ListenAndServe(":8000", r)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// StartServer initializes all controllers, and starts the HTTP server.
func StartServer() {
	file, err := os.OpenFile("/Users/boppudiaarshasai/Documents/Notification_Server/logfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)
	messageController, blacklistController, elasticSearchController := InitializeControllers()
	SetupRouter(messageController, blacklistController, elasticSearchController)
}
