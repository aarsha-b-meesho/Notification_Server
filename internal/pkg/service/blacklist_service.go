package service

import (
	"context"
	"fmt"
	"log"
	repository "notifications/internal/pkg/repository"
)

// Constants for Redis keys
const (
	BlacklistKey = "Black"
)

// BlacklistService provides methods to manage blacklisted phone numbers
type BlacklistService struct {
	db        *repository.MySQLRepo
	redisRepo *repository.RedisRepo
}

// NewBlacklistService creates a new instance of BlacklistService
func GetNewBlackListSerevice() *BlacklistService {
	sqlDb, err := repository.GetMySqlRepository()
	if err != nil {
		log.Panic("Error in getting sql db connection")

	}
	redisrepo, err := repository.GetRedisRepository()
	if err != nil {
		log.Panic("Error in getting sql db connection")

	}
	return &BlacklistService{
		db:        sqlDb,
		redisRepo: redisrepo,
	}
}

// AddToBlacklist adds phone numbers to the blacklist and returns the results
func (b *BlacklistService) AddToBlacklist(numbers []string) ([]string, []string, error) {
	var successfullyBlacklisted []string
	var alreadyBlacklisted []string
	ctx := context.Background()

	for _, number := range numbers {
		if isBlacklisted, err := b.isNumberBlacklisted(ctx, number); err != nil {
			return nil, nil, err
		} else if isBlacklisted {
			alreadyBlacklisted = append(alreadyBlacklisted, number)
		} else {
			if err := b.AddNumberToBlacklist(ctx, number); err != nil {
				return nil, nil, err
			}
			successfullyBlacklisted = append(successfullyBlacklisted, number)
		}
	}

	return successfullyBlacklisted, alreadyBlacklisted, nil
}

// RemoveFromBlacklist removes a phone number from the blacklist
func (b *BlacklistService) RemoveFromBlacklist(number string) error {
	ctx := context.Background()

	if isBlacklisted, err := b.isNumberBlacklisted(ctx, number); err != nil {
		return err
	} else if !isBlacklisted {
		return fmt.Errorf("number %s is not blacklisted", number)
	}

	return b.removeNumberFromBlacklist(ctx, number)
}

// GetAllFromBlacklist retrieves all blacklisted phone numbers
func (b *BlacklistService) GetAllFromBlacklist(ctx context.Context) ([]string, error) {
	blacklist, err := b.redisRepo.SMembers(ctx, BlacklistKey).Result()
	if err != nil {
		log.Printf("Error retrieving blacklisted numbers: %v", err)
		return nil, fmt.Errorf("error retrieving blacklisted numbers: %w", err)
	}
	return blacklist, nil
}

// IsNumberBlacklisted checks if a phone number is blacklisted
func (b *BlacklistService) IsNumberBlacklisted(ctx context.Context, number string) (bool, error) {
	return b.isNumberBlacklisted(ctx, number)
}

// Helper method to check if a number is blacklisted
func (b *BlacklistService) isNumberBlacklisted(ctx context.Context, number string) (bool, error) {
	isBlacklisted, err := b.redisRepo.SIsMember(ctx, BlacklistKey, number).Result()
	if err != nil {
		log.Printf("Error checking blacklist status for number %s: %v", number, err)
		return false, fmt.Errorf("error checking blacklist status for number %s: %w", number, err)
	}
	return isBlacklisted, nil
}

// Helper method to add a number to the blacklist
func (b *BlacklistService) AddNumberToBlacklist(ctx context.Context, number string) error {
	err := b.redisRepo.SAdd(ctx, BlacklistKey, number).Err()
	if err != nil {
		log.Printf("Error adding number %s to blacklist: %v", number, err)
		return fmt.Errorf("error adding number %s to blacklist: %w", number, err)
	}
	return nil
}

// Helper method to remove a number from the blacklist
func (b *BlacklistService) removeNumberFromBlacklist(ctx context.Context, number string) error {
	err := b.redisRepo.SRem(ctx, BlacklistKey, number).Err()
	if err != nil {
		log.Printf("Error removing number %s from blacklist: %v", number, err)
		return fmt.Errorf("error removing number %s from blacklist: %w", number, err)
	}
	log.Printf("Number %s successfully removed from blacklist", number)
	return nil
}
