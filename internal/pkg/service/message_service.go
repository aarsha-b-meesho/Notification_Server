package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	config "notifications/configurations"
	"notifications/internal/models"
	"notifications/internal/pkg/repository"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Constants for error messages
const (
	ErrCreateSMSDB        = "error inserting SMS into database"
	ErrProduceKafka       = "error producing message to Kafka"
	ErrSubscribeKafka     = "failed to subscribe to Kafka topic"
	ErrReadMessageKafka   = "failed to read message from Kafka"
	ErrCheckIDExistence   = "failed to check ID existence"
	ErrRetrieveSMSDetails = "failed to retrieve SMS details"
	ErrBlacklistCheck     = "error checking phone number existence"
	ErrUpdateSMSStatus    = "failed to update SMS details"
	ErrIndexElasticsearch = "failed to index SMS in Elasticsearch"
	ErrNoMessages         = "no messages to process"
	ErrParseTimestamp     = "error parsing timestamp"
)

type Message_Service struct {
	db              *repository.MySQLRepo
	producer        *kafka.Producer
	kafkaConsumer   *kafka.Consumer
	redisRepo       *repository.RedisRepo
	esRepo          *repository.ElasticsearchRepo
	queueMu         sync.Mutex
	processingQueue []string
	incomingQueue   []string
	queueSwitch     bool
}

var MSGS []models.SMS

func New_Message_Service(db *repository.MySQLRepo, producer *kafka.Producer, kafkaConsumer *kafka.Consumer, redisRepo *repository.RedisRepo, esRepo *repository.ElasticsearchRepo) *Message_Service {
	service := &Message_Service{
		db:            db,
		producer:      producer,
		kafkaConsumer: kafkaConsumer,
		redisRepo:     redisRepo,
		esRepo:        esRepo,
	}
	go service.StartConsumingMessages()
	return service
}

var now time.Time

func (s *Message_Service) Create_SMS(sms *models.SMS) error {
	now = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	sms.CreatedAt = now
	sms.UpdatedAt = now
	if err := s.db.Create(sms); err != nil {
		log.Printf("Create_SMS: %s: %v", ErrCreateSMSDB, err)
		return fmt.Errorf("%s: %w", ErrCreateSMSDB, err)
	}
	topic := "SMS"
	err := s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(sms.ID),
	}, nil)

	if err != nil {
		log.Printf("Create_SMS: %s: %v", ErrProduceKafka, err)
		return fmt.Errorf("%s: %w", ErrProduceKafka, err)
	}

	log.Printf("Create_SMS: Successfully created SMS with ID %s and sent to Kafka", sms.ID)
	MSGS = append(MSGS, *sms)
	return nil
}

func (s *Message_Service) StartConsumingMessages() {
	topic := config.KafkaTopic
	err := s.kafkaConsumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("StartConsumingMessages: %s: %v", ErrSubscribeKafka, err)
	}
	for {
		msg, err := s.kafkaConsumer.ReadMessage(-1)
		if err != nil {
			log.Printf("StartConsumingMessages: %s: %v", ErrReadMessageKafka, err)
			continue
		}
		s.queueMu.Lock()
		if s.queueSwitch {
			s.incomingQueue = append(s.incomingQueue, string(msg.Value))
		} else {
			s.processingQueue = append(s.processingQueue, string(msg.Value))
		}
		s.queueMu.Unlock()
		log.Printf("StartConsumingMessages: Message received from Kafka and added to queue: %s", msg.Value)
	}
}

func (s *Message_Service) Process_Messages() ([]map[string]interface{}, error) {
	s.queueMu.Lock()
	var messages []string
	if s.queueSwitch {
		messages = s.incomingQueue
		s.incomingQueue = nil
	} else {
		messages = s.processingQueue
		s.processingQueue = nil
	}
	s.queueSwitch = !s.queueSwitch
	s.queueMu.Unlock()

	if len(messages) == 0 {
		log.Println(ErrNoMessages)
		return s.create_no_messages_response(), nil
	}

	log.Printf("Process_Messages: Processing %d messages", len(messages))
	var results []map[string]interface{}
	for _, msgID := range messages {
		result, err := s.process_message(msgID)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	log.Println("Process_Messages: Message processing complete")
	return results, nil
}

func (s *Message_Service) create_no_messages_response() []map[string]interface{} {
	return []map[string]interface{}{
		{"data": map[string]string{"comments": ErrNoMessages}},
	}
}

func (s *Message_Service) process_message(msgID string) (map[string]interface{}, error) {
	exists, err := s.check_sms_existence(msgID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return s.create_sms_not_found_response(msgID), nil
	}

	sms, err := s.retrieve_sms_details(msgID)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	blacklist, err := s.check_blacklist_status(ctx, sms.PhoneNumber)
	if err != nil {
		return nil, err
	}

	if blacklist {
		return s.handle_blacklisted_sms(sms)
	}

	return s.handle_successful_sms(sms)
}

func (s *Message_Service) check_sms_existence(msgID string) (bool, error) {
	var exists bool
	existQuery := `SELECT EXISTS(SELECT 1 FROM sms WHERE id = ?)`
	err := s.db.Raw(existQuery, msgID).Row().Scan(&exists)
	if err != nil {
		log.Printf("check_sms_existence: %s for %s: %v", ErrCheckIDExistence, msgID, err)
		return false, fmt.Errorf("%s: %w", ErrCheckIDExistence, err)
	}
	return exists, nil
}

func (s *Message_Service) create_sms_not_found_response(msgID string) map[string]interface{} {
	return map[string]interface{}{
		"error": map[string]string{"comments": msgID},
	}
}

func (s *Message_Service) retrieve_sms_details(msgID string) (models.SMS, error) {
	var sms models.SMS
	query := `SELECT id, phone_number, message FROM sms WHERE id = ?`
	err := s.db.Raw(query, msgID).Scan(&sms).Error
	if err != nil {
		log.Printf("retrieve_sms_details: %s for %s: %v", ErrRetrieveSMSDetails, msgID, err)
		return sms, fmt.Errorf("%s: %w", ErrRetrieveSMSDetails, err)
	}
	return sms, nil
}

func (s *Message_Service) check_blacklist_status(ctx context.Context, phoneNumber string) (bool, error) {
	blacklist, err := s.redisRepo.SIsMember(ctx, "Black", phoneNumber).Result()
	if err != nil {
		log.Printf("check_blacklist_status: %s for %s: %v", ErrBlacklistCheck, phoneNumber, err)
		return false, fmt.Errorf("%s: %w", ErrBlacklistCheck, err)
	}
	return blacklist, nil
}

func (s *Message_Service) handle_blacklisted_sms(sms models.SMS) (map[string]interface{}, error) {
	sms.Status = "Failed"
	sms.FailureComments = "Blacklisted number"
	err := s.db.UpdateSMSStatus(sms.ID, sms.Status, sms.FailureComments)
	if err != nil {
		log.Printf("handle_blacklisted_sms: %s for %s: %v", ErrUpdateSMSStatus, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrUpdateSMSStatus, err)
	}

	err = s.index_sms_in_elasticsearch(sms)
	if err != nil {
		log.Printf("handle_blacklisted_sms: %s for %s: %v", ErrIndexElasticsearch, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrIndexElasticsearch, err)
	}

	log.Printf("handle_blacklisted_sms: SMS ID %s is blacklisted", sms.ID)
	return map[string]interface{}{
		"data": map[string]string{"comments": "Blacklisted number"},
	}, nil
}

func (s *Message_Service) handle_successful_sms(sms models.SMS) (map[string]interface{}, error) {
	sms.Status = "Successful"
	sms.FailureComments = "No failure comments"
	err := s.db.UpdateSMSStatus(sms.ID, sms.Status, sms.FailureComments)
	if err != nil {
		log.Printf("handle_successful_sms: %s for %s: %v", ErrUpdateSMSStatus, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrUpdateSMSStatus, err)
	}

	err = s.index_sms_in_elasticsearch(sms)
	if err != nil {
		log.Printf("handle_successful_sms: %s for %s: %v", ErrIndexElasticsearch, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrIndexElasticsearch, err)
	}

	log.Printf("handle_successful_sms: SMS ID %s processed successfully", sms.ID)
	return map[string]interface{}{
		"data": map[string]string{"comments": "Successfully processed"},
	}, nil
}

func (s *Message_Service) index_sms_in_elasticsearch(sms models.SMS) error {
	t := time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	dur := t.Sub(now)
	err := s.esRepo.CreateIndexSMS(sms, dur)
	if err != nil {
		return fmt.Errorf("failed to index SMS in Elasticsearch: %w", err)
	}
	return nil
}


func (s *Message_Service) Check_ID_Exists(msgID string) (bool, error) {
	var exists bool

	// Query to check if the SMS ID exists
	existQuery := `SELECT EXISTS(SELECT 1 FROM sms WHERE id = ?)`
	err := s.db.Raw(existQuery, msgID).Row().Scan(&exists)
	if err != nil {
		log.Printf("Check_ID_Exists: Failed to check ID existence for %s: %v", msgID, err)
		return false, fmt.Errorf("failed to check ID existence: %w", err)
	}

	return exists, nil
}

func (s *Message_Service) Get_All_Messages() ([]models.SMS, error) {
	query := `SELECT id, phone_number, message, status, failure_code, failure_comments, created_at, updated_at FROM sms`

	rows, err := s.db.Raw(query).Rows()
	if err != nil {
		log.Printf("Get_All_Messages: Failed to retrieve SMS details: %v", err)
		return nil, fmt.Errorf("failed to retrieve SMS details: %w", err)
	}
	defer rows.Close()

	var result []models.SMS
	for rows.Next() {
		var sms models.SMS
		var createdAt, updatedAt []uint8

		if err := rows.Scan(&sms.ID, &sms.PhoneNumber, &sms.Message, &sms.Status, &sms.FailureCode, &sms.FailureComments, &createdAt, &updatedAt); err != nil {
			log.Printf("Get_All_Messages: Error scanning row: %v", err)
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		parsedCreatedAt, parsedUpdatedAt, err := s.parse_Timestamps(createdAt, updatedAt)
		if err != nil {
			log.Printf("Get_All_Messages: %v", err)
			return nil, err
		}

		sms.CreatedAt = parsedCreatedAt
		sms.UpdatedAt = parsedUpdatedAt

		result = append(result, sms)
	}

	return result, nil
}
func (s *Message_Service) parse_Timestamps(createdAt, updatedAt []uint8) (time.Time, time.Time, error) {
	var parsedCreatedAt, parsedUpdatedAt time.Time

	var err error
	if len(createdAt) > 0 {
		parsedCreatedAt, err = time.Parse("2006-01-02 15:04:05", string(createdAt))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error parsing CreatedAt: %w", err)
		}
	}

	if len(updatedAt) > 0 {
		parsedUpdatedAt, err = time.Parse("2006-01-02 15:04:05", string(updatedAt))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error parsing UpdatedAt: %w", err)
		}
	}

	return parsedCreatedAt, parsedUpdatedAt, nil
}
func (s *Message_Service) Get_Message_By_ID(msgID string) (*models.SMS, error) {
	var sms models.SMS
	var createdAt, updatedAt []uint8

	query := `SELECT id, phone_number, message, status, failure_code, failure_comments, created_at, updated_at FROM sms WHERE id = ?`
	err := s.db.Raw(query, msgID).Row().Scan(
		&sms.ID,
		&sms.PhoneNumber,
		&sms.Message,
		&sms.Status,
		&sms.FailureCode,
		&sms.FailureComments,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		log.Printf("Get_Message_By_ID: Failed to retrieve SMS details for %s: %v", msgID, err)
		return nil, fmt.Errorf("failed to retrieve SMS details: %w", err)
	}

	parsedCreatedAt, parsedUpdatedAt, err := s.parse_Timestamps(createdAt, updatedAt)
	if err != nil {
		log.Printf("Get_Message_By_ID: %v", err)
		return nil, err
	}

	sms.CreatedAt = parsedCreatedAt
	sms.UpdatedAt = parsedUpdatedAt

	return &sms, nil
}
