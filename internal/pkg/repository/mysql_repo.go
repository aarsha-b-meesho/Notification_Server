package repository

import (
	"fmt"
	"log"
	"notifications/internal/models"
	"time"
	"notifications/configurations"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySQLRepo struct {
	db *gorm.DB
}

func NewMySQL(dsn string) (*MySQLRepo, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("NewMySQLRepo: Failed to open database: %v", err)
		return nil, err
	}
	return &MySQLRepo{db: db}, nil
}

func (r *MySQLRepo) Migrate() error {
	if err := r.db.Migrator().DropTable(&models.SMS{}); err != nil {
		log.Printf("Migrate: Failed to drop existing tables: %v", err)
		return fmt.Errorf("failed to drop tables: %w", err)
	}
	log.Println("Migrate: Dropped existing tables successfully")

	// AutoMigrate the SMS model
	if err := r.db.AutoMigrate(&models.SMS{}); err != nil {
		log.Printf("Migrate: Failed to migrate database: %v", err)
		return fmt.Errorf("failed to migrate database: %w", err)
	}
	log.Println("Migrate: Database migration completed successfully")

	return nil
}

func (r *MySQLRepo) Create(s *models.SMS) error {
	if err := r.db.Create(s).Error; err != nil {
		log.Printf("Create: Failed to create SMS record: %v", err)
		return err
	}
	log.Println("Create: SMS record created successfully")
	return nil
}

func (r *MySQLRepo) Raw(query string, args ...interface{}) *gorm.DB {
	log.Printf("Raw: Executing query: %s, args: %v", query, args)
	return r.db.Raw(query, args...)
}

func (r *MySQLRepo) UpdateSMSStatus(id, status, failureComments string) error {
	if err := r.db.Model(&models.SMS{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":           status,
		"failure_comments": failureComments,
		"updated_at":       time.Now().UTC().Add(5*time.Hour + 30*time.Minute),
	}).Error; err != nil {
		log.Printf("UpdateSMSStatus: Failed to update SMS status for ID %s: %v", id, err)
		return err
	}
	log.Printf("UpdateSMSStatus: Updated SMS status for ID %s successfully", id)
	return nil
}
func GetMySqlRepository() (*MySQLRepo, error) {
	// Initialize MySQL repository
	mySQLRepo, err := NewMySQL(config.MySQLDSN)
	if err != nil {
		return nil, err
	}

	// Migrate DB
	if err := mySQLRepo.Migrate(); err != nil {
		return nil, err
	}
	return mySQLRepo, nil
}
