package initations

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"notifications/configurations"
	"notifications/internal/kafka"
	controllers "notifications/internal/pkg/controllers"
	"notifications/internal/pkg/repository"
	service "notifications/internal/pkg/service"
	"os"

	"github.com/gorilla/mux"
)

// InitDependencies initializes all dependencies and returns them.
func InitDependencies() (
	*repository.RedisRepo, 
	*repository.MySQLRepo, 
	*repository.ElasticsearchRepo, 
	*kafka.Producer, 
	*kafka.Consumer, 
	error,
) {
	// Initialize Redis repository
	redisRepo := repository.New_Redis_Repo(config.RedisAddr)
	ctx := context.Background()

	// Initialize MySQL repository
	mySQLRepo, err := repository.New_MySQL_Repo(config.MySQLDSN)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Migrate DB
	if err := mySQLRepo.Migrate(); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Ping Redis to check connection
	if err := redisRepo.Ping(ctx); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Flush Redis database (optional, based on use case)
	if err := redisRepo.FlushDB(ctx); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Initialize Kafka producer
	producer, err := kafka.New_Producer(config.KafkaAddr)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Initialize Kafka consumer
	kafkaConsumer, err := kafka.New_Consumer(config.KafkaAddr, config.KafkaTopic)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Initialize Elasticsearch repository
	elasticRepo, err := repository.New_Elastic_Search_Repo(config.ElasticsearchAddr)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	if err := elasticRepo.CreateIndex("sms_index"); err != nil {
		return nil, nil, nil, nil, nil, err
	}

	return redisRepo, mySQLRepo, elasticRepo, producer, kafkaConsumer, nil
}

// InitializeServices initializes all services and returns them.
func InitializeServices(
	mySQLRepo *repository.MySQLRepo,
	redisRepo *repository.RedisRepo,
	elasticRepo *repository.ElasticsearchRepo,
	producer *kafka.Producer,
	kafkaConsumer *kafka.Consumer,
) (
	*service.Message_Service,
	*service.Blacklist_Service,
	*service.Elasticsearch_Service,
	error, 
) {
	// Initialize services
	smsService := service.New_Message_Service(mySQLRepo, producer, kafkaConsumer, redisRepo, elasticRepo)
	blacklistService := service.New_Blacklist_Service(mySQLRepo, redisRepo)
	elasticService := service.New_ElasticSearch_Service(elasticRepo)

	return smsService, blacklistService, elasticService, nil
}

// InitializeControllers initializes the controllers and returns them.
func InitializeControllers(
	smsService *service.Message_Service,
	blacklistService *service.Blacklist_Service,
	elasticService *service.Elasticsearch_Service,
) (
	*controllers.Message_Controller,
	*controllers.BlackList_Controller,
	*controllers.Elastic_search_Controller,
) {
	smsHandler := controllers.New_Message_Controller(smsService)
	blacklistHandler := controllers.New_Blacklist_Controller(blacklistService)
	elasticHandler := controllers.New_ElasticSearch_Controller(elasticService)

	return smsHandler, blacklistHandler, elasticHandler
}

// Setup_Router sets up the HTTP routes and starts the server.
func Setup_Router(
	Message_Controller *controllers.Message_Controller, 
	BlackList_Controller *controllers.BlackList_Controller, 
	Elastic_search_Controller *controllers.Elastic_search_Controller,
) {
	r := mux.NewRouter()

	// Define routes
	r.HandleFunc("/blacklist", BlackList_Controller.Add_Number_To_BlackList).Methods("POST")
	r.HandleFunc("/blacklist", BlackList_Controller.Get_All_From_BlackList).Methods("GET")
	r.HandleFunc("/blacklist/{number}", BlackList_Controller.Delete_From_BlackList).Methods("DELETE")
	r.HandleFunc("/blacklist/{number}", BlackList_Controller.Get_BlackList_By_ID).Methods("GET")
	r.HandleFunc("/sms/{ID}", Message_Controller.Get_Message_By_Id).Methods("GET")
	r.HandleFunc("/sms", Message_Controller.Notify_Server).Methods("POST")
	r.HandleFunc("/sms", Message_Controller.Get_All_Messages).Methods("GET")
	r.HandleFunc("/notify", Message_Controller.Send_Message).Methods("GET")
	r.HandleFunc("/elastic/{id}", Elastic_search_Controller.Get_Doc_By_ID).Methods("GET")
	r.HandleFunc("/elastic", Elastic_search_Controller.Get_All_Docs).Methods("GET")
	r.HandleFunc("/elastictext/{text}", Elastic_search_Controller.Get_Doc_By_Text).Methods("GET")
	r.HandleFunc("/elasticsearchbytime", Elastic_search_Controller.Get_Doc_By_TimeRange).Methods("GET")
	fmt.Println("Starting the server at :8000")
	log.Println("Starting the server at :8000")
	
	err := http.ListenAndServe(":8000", r)
	if err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// Start_Server initializes all dependencies, services, controllers, and starts the HTTP server.
func Start_Server() {
	file, err := os.OpenFile("/Users/boppudiaarshasai/Documents/Notification_Server/logfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)

	redisRepo, mySQLRepo, elasticRepo, producer, kafkaConsumer, err := InitDependencies()
	if err != nil {
		log.Fatalf("Failed to initialize dependencies: %v", err)
	}

	// Initialize services
	smsService, blacklistService, elasticService, err := InitializeServices(mySQLRepo, redisRepo, elasticRepo, producer, kafkaConsumer)
	if err != nil {
		log.Fatalf("Failed to initialize services: %v", err)
	}

	// Initialize controllers
	smsHandler, blacklistHandler, elasticHandler := InitializeControllers(smsService, blacklistService, elasticService)

	Setup_Router(smsHandler, blacklistHandler, elasticHandler)
}
