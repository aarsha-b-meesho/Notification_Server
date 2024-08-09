package service

import (
	"context"
	"fmt"
	"log"
	"time"

	config "notifications/configurations"
	"notifications/internal/models"
	"notifications/internal/pkg/repository"
	msg "notifications/internal/kafka"
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

type MessageService struct {
	db              *repository.MySQLRepo
	producer        *kafka.Producer
	kafkaConsumer   *kafka.Consumer
	redisRepo       *repository.RedisRepo
	esRepo          *repository.ElasticsearchRepo
	processingQueue []string
	incomingQueue   []string
	queueSwitch     bool
}

var MSGS []models.SMS

func GetMessageService() *MessageService {
	sqlrepo,err:= repository.GetMySqlRepository()
	if err!=nil{
		log.Panic(err)
	}
	produce,err:=msg.GetKafkaProducer()
	if err!=nil{
		log.Panic(err)
	}
	consume,err:= msg.GetKafkaConsumer()
	if err!=nil{
		log.Panic(err)
	}
	redisrepo,err:= repository.GetRedisRepository()
	if err!=nil{
		log.Panic(err)
	}
	elasticrepo,err:= repository.GetElasticRepo()
	if err!=nil{
		log.Panic(err)
	}
	service := &MessageService{
		db:            sqlrepo,
		producer:      produce,
		kafkaConsumer: consume,
		redisRepo:     redisrepo,
		esRepo:        elasticrepo,
	}
	go service.StartConsumingMessages()
	return service
}

var now time.Time

func (s *MessageService) CreateMessage(sms *models.SMS) error {
	now = time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	sms.CreatedAt = now
	sms.UpdatedAt = now
	if err := s.db.Create(sms); err != nil {
		log.Printf("CreateMessage: %s: %v", ErrCreateSMSDB, err)
		return fmt.Errorf("%s: %w", ErrCreateSMSDB, err)
	}
	topic := "SMS"
	err := s.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(sms.ID),
	}, nil)

	if err != nil {
		log.Printf("CreateMessage: %s: %v", ErrProduceKafka, err)
		return fmt.Errorf("%s: %w", ErrProduceKafka, err)
	}

	log.Printf("CreateMessage: Successfully created SMS with ID %s and sent to Kafka", sms.ID)
	MSGS = append(MSGS, *sms)
	return nil
}

func (s *MessageService) StartConsumingMessages() {
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

		if s.queueSwitch  {
			s.incomingQueue = append(s.incomingQueue, string(msg.Value))
		} else {
			s.processingQueue = append(s.processingQueue, string(msg.Value))
		}
		log.Printf("StartConsumingMessages: Message received from Kafka and added to queue: %s", msg.Value)
	}
}
func (s *MessageService) ProcessMessages() ([]map[string]interface{}, error) {
	s.queueSwitch =!s.queueSwitch
	var messages []string

	// Check the current queueSwitch value atomically
	if s.queueSwitch {
		messages = s.processingQueue
		s.processingQueue = nil
	} else {
		messages = s.incomingQueue
		s.incomingQueue = nil
	}

	if len(messages) == 0 {
		log.Println(ErrNoMessages)
		return s.createNoMessagesResponse(), nil
	}

	log.Printf("ProcessMessages: Processing %d messages", len(messages))
	var results []map[string]interface{}
	for _, msgID := range messages {
		result, err := s.processMessage(msgID)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	log.Println("ProcessMessages: Message processing complete")
	return results, nil
}

func (s *MessageService) createNoMessagesResponse() []map[string]interface{} {
	return []map[string]interface{}{
		{"data": map[string]string{"comments": ErrNoMessages}},
	}
}

func (s *MessageService) processMessage(msgID string) (map[string]interface{}, error) {
	exists, err := s.checkSmsExistence(msgID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return s.createSmsNotFoundResponse(msgID), nil
	}

	sms, err := s.retrieveSmsDetails(msgID)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	blacklist, err := s.checkBlacklistStatus(ctx, sms.PhoneNumber)
	if err != nil {
		return nil, err
	}

	if blacklist {
		return s.handleBlacklistedSms(sms)
	}

	return s.handleSuccessfulSms(sms)
}

func (s *MessageService) checkSmsExistence(msgID string) (bool, error) {
	var exists bool
	existQuery := `SELECT EXISTS(SELECT 1 FROM sms WHERE id = ?)`
	err := s.db.Raw(existQuery, msgID).Row().Scan(&exists)
	if err != nil {
		log.Printf("checkSmsExistence: %s for %s: %v", ErrCheckIDExistence, msgID, err)
		return false, fmt.Errorf("%s: %w", ErrCheckIDExistence, err)
	}
	return exists, nil
}

func (s *MessageService) createSmsNotFoundResponse(msgID string) map[string]interface{} {
	return map[string]interface{}{
		"error": map[string]string{"comments": msgID},
	}
}

func (s *MessageService) retrieveSmsDetails(msgID string) (models.SMS, error) {
	var sms models.SMS
	query := `SELECT id, phone_number, message FROM sms WHERE id = ?`
	err := s.db.Raw(query, msgID).Scan(&sms).Error
	if err != nil {
		log.Printf("retrieveSmsDetails: %s for %s: %v", ErrRetrieveSMSDetails, msgID, err)
		return sms, fmt.Errorf("%s: %w", ErrRetrieveSMSDetails, err)
	}
	return sms, nil
}

func (s *MessageService) checkBlacklistStatus(ctx context.Context, phoneNumber string) (bool, error) {
	blacklist, err := s.redisRepo.SIsMember(ctx, "Black", phoneNumber).Result()
	if err != nil {
		log.Printf("checkBlacklistStatus: %s for %s: %v", ErrBlacklistCheck, phoneNumber, err)
		return false, fmt.Errorf("%s: %w", ErrBlacklistCheck, err)
	}
	return blacklist, nil
}

func (s *MessageService) handleBlacklistedSms(sms models.SMS) (map[string]interface{}, error) {
	sms.Status = "Failed"
	sms.FailureComments = "Blacklisted number"
	err := s.db.UpdateSMSStatus(sms.ID, sms.Status, sms.FailureComments)
	if err != nil {
		log.Printf("handleBlacklistedSms: %s for %s: %v", ErrUpdateSMSStatus, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrUpdateSMSStatus, err)
	}

	err = s.indexSmsInElasticsearch(sms)
	if err != nil {
		log.Printf("handleBlacklistedSms: %s for %s: %v", ErrIndexElasticsearch, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrIndexElasticsearch, err)
	}

	log.Printf("handleBlacklistedSms: SMS ID %s is blacklisted", sms.ID)
	return map[string]interface{}{
		"data": map[string]string{"comments": "Blacklisted number"},
	}, nil
}

func (s *MessageService) handleSuccessfulSms(sms models.SMS) (map[string]interface{}, error) {
	sms.Status = "Successful"
	sms.FailureComments = "No failure comments"
	err := s.db.UpdateSMSStatus(sms.ID, sms.Status, sms.FailureComments)
	if err != nil {
		log.Printf("handleSuccessfulSms: %s for %s: %v", ErrUpdateSMSStatus, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrUpdateSMSStatus, err)
	}

	err = s.indexSmsInElasticsearch(sms)
	if err != nil {
		log.Printf("handleSuccessfulSms: %s for %s: %v", ErrIndexElasticsearch, sms.ID, err)
		return nil, fmt.Errorf("%s: %w", ErrIndexElasticsearch, err)
	}

	log.Printf("handleSuccessfulSms: SMS ID %s processed successfully", sms.ID)
	return map[string]interface{}{
		"data": map[string]string{"comments": "Successfully processed"},
	}, nil
}

func (s *MessageService) indexSmsInElasticsearch(sms models.SMS) error {
	t := time.Now().UTC().Add(5*time.Hour + 30*time.Minute)
	dur := t.Sub(now)
	err := s.esRepo.CreateIndexSMS(sms, dur)
	if err != nil {
		return fmt.Errorf("failed to index SMS in Elasticsearch: %w", err)
	}
	return nil
}

func (s *MessageService) CheckIDExists(msgID string) (bool, error) {
	var exists bool

	// Query to check if the SMS ID exists
	existQuery := `SELECT EXISTS(SELECT 1 FROM sms WHERE id = ?)`
	err := s.db.Raw(existQuery, msgID).Row().Scan(&exists)
	if err != nil {
		log.Printf("CheckIDExists: Failed to check ID existence for %s: %v", msgID, err)
		return false, fmt.Errorf("failed to check ID existence: %w", err)
	}

	return exists, nil
}

func (s *MessageService) GetAllMessages() ([]models.SMS, error) {
	query := `SELECT id, phone_number, message, status, failure_code, failure_comments, created_at, updated_at FROM sms`

	rows, err := s.db.Raw(query).Rows()
	if err != nil {
		log.Printf("GetAllMessages: Failed to retrieve SMS details: %v", err)
		return nil, fmt.Errorf("failed to retrieve SMS details: %w", err)
	}
	defer rows.Close()

	var result []models.SMS
	for rows.Next() {
		var sms models.SMS
		var createdAt, updatedAt []uint8

		if err := rows.Scan(&sms.ID, &sms.PhoneNumber, &sms.Message, &sms.Status, &sms.FailureCode, &sms.FailureComments, &createdAt, &updatedAt); err != nil {
			log.Printf("GetAllMessages: Error scanning row: %v", err)
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		parsedCreatedAt, parsedUpdatedAt, err := s.parseTimestamps(createdAt, updatedAt)
		if err != nil {
			log.Printf("GetAllMessages: %v", err)
			return nil, err
		}

		sms.CreatedAt = parsedCreatedAt
		sms.UpdatedAt = parsedUpdatedAt

		result = append(result, sms)
	}

	return result, nil
}
func (s *MessageService) parseTimestamps(createdAt, updatedAt []uint8) (time.Time, time.Time, error) {
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
func (s *MessageService) GetMessageByID(msgID string) (*models.SMS, error) {
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
		log.Printf("GetMessageByID: Failed to retrieve SMS details for %s: %v", msgID, err)
		return nil, fmt.Errorf("failed to retrieve SMS details: %w", err)
	}

	parsedCreatedAt, parsedUpdatedAt, err := s.parseTimestamps(createdAt, updatedAt)
	if err != nil {
		log.Printf("GetMessageByID: %v", err)
		return nil, err
	}

	sms.CreatedAt = parsedCreatedAt
	sms.UpdatedAt = parsedUpdatedAt

	return &sms, nil
}
