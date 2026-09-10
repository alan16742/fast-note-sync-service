// Package service implements the business logic layer
// Package service 实现业务逻辑层
package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/domain"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	"github.com/haierkeys/fast-note-sync-service/pkg/logger"
	"github.com/haierkeys/fast-note-sync-service/pkg/util"
	"go.uber.org/zap"
)

// ConflictService defines the conflict service interface
// ConflictService 定义冲突文件服务接口
type ConflictService interface {
	// CreateConflictFile creates a conflict file
	// CreateConflictFile 创建冲突文件
	// When merging fails, save the client content as a conflict file
	// 当合并失败时，将客户端内容保存为冲突文件
	CreateConflictFile(ctx context.Context, uid int64, params *dto.ConflictFileRequest) (*dto.ConflictFileResponse, error)
}

// conflictService implements ConflictService interface
// conflictService 实现 ConflictService 接口
type conflictService struct {
	noteRepo       domain.NoteRepository
	vaultService   VaultService
	syncLogService SyncLogService
	eventPublisher NoteEventPublisher
	logger         *zap.Logger
	clientName     string
	clientType     string
}

// NewConflictService creates a ConflictService instance
// NewConflictService 创建 ConflictService 实例
func NewConflictService(
	noteRepo domain.NoteRepository,
	vaultSvc VaultService,
	syncLogService SyncLogService,
	eventPublisher NoteEventPublisher,
	logger *zap.Logger,
) ConflictService {
	return &conflictService{
		noteRepo:       noteRepo,
		vaultService:   vaultSvc,
		syncLogService: syncLogService,
		eventPublisher: eventPublisher,
		logger:         logger,
		clientName:     "conflict-service",
		clientType:     "server",
	}
}

// CreateConflictFile creates a conflict file
// CreateConflictFile 创建冲突文件
func (s *conflictService) CreateConflictFile(ctx context.Context, uid int64, params *dto.ConflictFileRequest) (*dto.ConflictFileResponse, error) {
	// Get VaultID
	// 获取 VaultID
	vaultID, err := s.vaultService.MustGetID(ctx, uid, params.Vault)
	if err != nil {
		return nil, err
	}

	// Generate conflict file path
	// 生成冲突文件路径
	conflictPath := s.generateConflictPath(params.OriginalPath)

	// Generate conflict path hash
	// 生成冲突路径哈希
	conflictPathHash := util.EncodeHash32(conflictPath)

	// Create conflict file note
	// 创建冲突文件笔记
	conflictNote := &domain.Note{
		VaultID:     vaultID,
		Path:        conflictPath,
		PathHash:    conflictPathHash,
		Content:     params.ClientContent,
		ContentHash: params.ClientContentHash,
		ClientName:  s.clientName,
		ClientType:  s.clientType,
		Size:        int64(len(params.ClientContent)),
		Mtime:       params.Mtime,
		Ctime:       params.Ctime,
		Action:      domain.NoteActionCreate,
	}

	// Save conflict file
	// 保存冲突文件
	created, err := s.noteRepo.Create(ctx, conflictNote, uid)
	if err != nil {
		s.logger.Error("failed to create conflict file",
			zap.String("originalPath", params.OriginalPath),
			zap.String("conflictPath", conflictPath),
			zap.Int64(logger.FieldUID, uid),
			zap.Error(err))
		return nil, code.ErrorDBQuery.WithDetails(err.Error())
	}

	s.logger.Info("conflict file created",
		zap.String("originalPath", params.OriginalPath),
		zap.String("conflictPath", conflictPath),
		zap.Int64(logger.FieldUID, uid),
		zap.Int64("noteId", created.ID))

	// The conflict copy is written straight through the repository, so record it
	// in the audit log and publish the note event that automation matches on.
	// Log() alone is not enough: it only publishes automation events for
	// file/folder types, never for notes.
	// 冲突副本直接经仓储写入，因此需要补写审计日志并发布自动化所匹配的笔记事件。
	// 仅调用 Log() 不够：它只为 file/folder 类型发布自动化事件，不会为笔记发布。
	if s.syncLogService != nil {
		s.syncLogService.Log(
			uid, vaultID, domain.SyncLogTypeNote, domain.SyncLogActionCreate, "",
			created.Path, created.PathHash, s.clientType, s.clientName, "", created.Size,
		)
	}
	publishNoteChangeEvent(ctx, s.eventPublisher, uid, vaultID, params.Vault, domain.WebhookActionCreate, created, "", "content", "ctime", "mtime")

	return &dto.ConflictFileResponse{
		ConflictPath: conflictPath,
		Message:      "合并失败，已保存冲突版本",
		NoteID:       created.ID,
	}, nil
}

// generateConflictPath generates conflict file path
// Format: {baseName}.conflict.{timestamp}{ext}
// Example: notes/test.md -> notes/test.conflict.20060102150405.md
// generateConflictPath 生成冲突文件路径
// 格式: {baseName}.conflict.{timestamp}{ext}
// 例如: notes/test.md -> notes/test.conflict.20060102150405.md
func (s *conflictService) generateConflictPath(originalPath string) string {
	timestamp := time.Now().Format("20060102150405")
	ext := filepath.Ext(originalPath)
	baseName := strings.TrimSuffix(originalPath, ext)
	return fmt.Sprintf("%s.conflict.%s%s", baseName, timestamp, ext)
}

// 确保 conflictService 实现了 ConflictService 接口
var _ ConflictService = (*conflictService)(nil)
