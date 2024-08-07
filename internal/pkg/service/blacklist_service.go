package service

import (
	"context"
	"fmt"
	"log"

	"notifications/internal/pkg/repository"
)

// Constants for Redis keys
const (
	BlacklistKey = "Black"
)

// Blacklist_Service provides methods to manage blacklisted phone numbers
type Blacklist_Service struct {
	db        *repository.MySQLRepo
	redisRepo *repository.RedisRepo
}

// New_Blacklist_Service creates a new instance of Blacklist_Service
func New_Blacklist_Service(db *repository.MySQLRepo, redisRepo *repository.RedisRepo) *Blacklist_Service {
	return &Blacklist_Service{
		db:        db,
		redisRepo: redisRepo,
	}
}

// Add_To_Blacklist adds phone numbers to the blacklist and returns the results
func (b *Blacklist_Service) Add_To_Blacklist(numbers []string) ([]string, []string, error) {
	var successfullyBlacklisted []string
	var alreadyBlacklisted []string
	ctx := context.Background()

	for _, number := range numbers {
		if isBlacklisted, err := b.isNumberBlacklisted(ctx, number); err != nil {
			return nil, nil, err
		} else if isBlacklisted {
			alreadyBlacklisted = append(alreadyBlacklisted, number)
		} else {
			if err := b.addNumberToBlacklist(ctx, number); err != nil {
				return nil, nil, err
			}
			successfullyBlacklisted = append(successfullyBlacklisted, number)
		}
	}

	return successfullyBlacklisted, alreadyBlacklisted, nil
}

// Remove_From_Blacklist removes a phone number from the blacklist
func (b *Blacklist_Service) Remove_From_Blacklist(number string) error {
	ctx := context.Background()

	if isBlacklisted, err := b.isNumberBlacklisted(ctx, number); err != nil {
		return err
	} else if !isBlacklisted {
		return fmt.Errorf("number %s is not blacklisted", number)
	}

	return b.removeNumberFromBlacklist(ctx, number)
}

// Get_All_Blacklisted_Numbers retrieves all blacklisted phone numbers
func (b *Blacklist_Service) Get_All_Blacklisted_Numbers(ctx context.Context) ([]string, error) {
	blacklist, err := b.redisRepo.SMembers(ctx, BlacklistKey).Result()
	if err != nil {
		log.Printf("Error retrieving blacklisted numbers: %v", err)
		return nil, fmt.Errorf("error retrieving blacklisted numbers: %w", err)
	}
	return blacklist, nil
}

// Is_Number_Blacklisted checks if a phone number is blacklisted
func (b *Blacklist_Service) Is_Number_Blacklisted(ctx context.Context, number string) (bool, error) {
	return b.isNumberBlacklisted(ctx, number)
}

// Helper method to check if a number is blacklisted
func (b *Blacklist_Service) isNumberBlacklisted(ctx context.Context, number string) (bool, error) {
	isBlacklisted, err := b.redisRepo.SIsMember(ctx, BlacklistKey, number).Result()
	if err != nil {
		log.Printf("Error checking blacklist status for number %s: %v", number, err)
		return false, fmt.Errorf("error checking blacklist status for number %s: %w", number, err)
	}
	return isBlacklisted, nil
}

// Helper method to add a number to the blacklist
func (b *Blacklist_Service) addNumberToBlacklist(ctx context.Context, number string) error {
	err := b.redisRepo.SAdd(ctx, BlacklistKey, number).Err()
	if err != nil {
		log.Printf("Error adding number %s to blacklist: %v", number, err)
		return fmt.Errorf("error adding number %s to blacklist: %w", number, err)
	}
	return nil
}

// Helper method to remove a number from the blacklist
func (b *Blacklist_Service) removeNumberFromBlacklist(ctx context.Context, number string) error {
	err := b.redisRepo.SRem(ctx, BlacklistKey, number).Err()
	if err != nil {
		log.Printf("Error removing number %s from blacklist: %v", number, err)
		return fmt.Errorf("error removing number %s from blacklist: %w", number, err)
	}
	log.Printf("Number %s successfully removed from blacklist", number)
	return nil
}
