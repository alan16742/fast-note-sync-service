package api_router

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/haierkeys/fast-note-sync-service/internal/app"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	apperrors "github.com/haierkeys/fast-note-sync-service/pkg/errors"
	"go.uber.org/zap"
)

// WebhookHandler manages user-owned webhook subscriptions.
type WebhookHandler struct{ *Handler }

// NewWebhookHandler creates a webhook handler.
func NewWebhookHandler(a *app.App) *WebhookHandler { return &WebhookHandler{Handler: NewHandler(a)} }

// List returns the current user's webhook subscriptions.
func (h *WebhookHandler) List(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	items, err := h.App.WebhookService.List(c.Request.Context(), uid)
	if err != nil {
		h.App.Logger().Warn("list webhook subscriptions failed", zap.Int64("uid", uid), zap.Error(err))
		apperrors.ErrorResponse(c, err)
		return
	}
	response.ToResponse(code.Success.WithData(items))
}

// Save creates or updates a webhook subscription.
func (h *WebhookHandler) Save(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	request := &dto.WebhookSubscriptionRequest{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	item, err := h.App.WebhookService.Save(c.Request.Context(), uid, request)
	if err != nil {
		h.App.Logger().Warn("save webhook subscription failed", zap.Int64("uid", uid), zap.Error(err))
		response.ToResponse(code.ErrorInvalidParams.WithDetails(err.Error()))
		return
	}
	response.ToResponse(code.SuccessUpdate.WithData(item))
}

// Delete removes a webhook subscription owned by the current user.
func (h *WebhookHandler) Delete(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	var request struct {
		ID int64 `json:"id" form:"id" binding:"required"`
	}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	if err := h.App.WebhookService.Delete(c.Request.Context(), uid, request.ID); err != nil {
		h.App.Logger().Warn("delete webhook subscription failed", zap.Int64("uid", uid), zap.Error(err))
		apperrors.ErrorResponse(c, err)
		return
	}
	response.ToResponse(code.Success)
}

// Test sends a synthetic message through a saved channel or current form configuration.
func (h *WebhookHandler) Test(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	request := &dto.WebhookSubscriptionRequest{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	if request.ID <= 0 && strings.TrimSpace(request.Provider) == "" {
		response.ToResponse(code.ErrorInvalidParams.WithDetails("webhook subscription id or configuration is required"))
		return
	}
	var err error
	if strings.TrimSpace(request.Provider) == "" && request.ID > 0 {
		err = h.App.WebhookService.Test(c.Request.Context(), uid, request.ID)
	} else {
		err = h.App.WebhookService.TestRequest(c.Request.Context(), uid, request)
	}
	if err != nil {
		h.App.Logger().Warn("test webhook subscription failed", zap.Int64("uid", uid), zap.Int64("subscription", request.ID), zap.Error(err))
		response.ToResponse(code.ErrorInvalidParams.WithDetails(err.Error()))
		return
	}
	response.ToResponse(code.Success)
}
