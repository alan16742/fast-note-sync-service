package api_router

import (
	"github.com/gin-gonic/gin"
	"github.com/haierkeys/fast-note-sync-service/internal/app"
	"github.com/haierkeys/fast-note-sync-service/internal/dto"
	"github.com/haierkeys/fast-note-sync-service/internal/middleware"
	pkgapp "github.com/haierkeys/fast-note-sync-service/pkg/app"
	"github.com/haierkeys/fast-note-sync-service/pkg/code"
	apperrors "github.com/haierkeys/fast-note-sync-service/pkg/errors"
	"go.uber.org/zap"
)

// AutomationHandler manages event conditions and target bindings.
type AutomationHandler struct{ *Handler }

func NewAutomationHandler(a *app.App) *AutomationHandler {
	return &AutomationHandler{Handler: NewHandler(a)}
}

func (h *AutomationHandler) List(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	items, err := h.App.AutomationService.List(c.Request.Context(), uid)
	if err != nil {
		h.logError(c, "AutomationHandler.List", err)
		apperrors.ErrorResponse(c, err)
		return
	}
	response.ToResponse(code.Success.WithData(items))
}

func (h *AutomationHandler) Save(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	request := &dto.AutomationTriggerRequest{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	item, err := h.App.AutomationService.Save(c.Request.Context(), uid, request)
	if err != nil {
		h.logError(c, "AutomationHandler.Save", err)
		response.ToResponse(code.ErrorInvalidParams.WithDetails(err.Error()))
		return
	}
	response.ToResponse(code.SuccessUpdate.WithData(item))
}

func (h *AutomationHandler) Delete(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	request := &struct {
		ID int64 `json:"id" form:"id" binding:"required"`
	}{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	if err := h.App.AutomationService.Delete(c.Request.Context(), uid, request.ID); err != nil {
		h.logError(c, "AutomationHandler.Delete", err)
		apperrors.ErrorResponse(c, err)
		return
	}
	response.ToResponse(code.Success)
}

func (h *AutomationHandler) Trigger(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	request := &dto.AutomationRunRequest{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	if err := h.App.AutomationService.Trigger(c.Request.Context(), uid, request); err != nil {
		h.logError(c, "AutomationHandler.Trigger", err)
		response.ToResponse(code.ErrorInvalidParams.WithDetails(err.Error()))
		return
	}
	response.ToResponse(code.Success.WithDetails("Automation trigger completed"))
}

func (h *AutomationHandler) logError(c *gin.Context, method string, err error) {
	h.App.Logger().Warn(method, zap.Error(err), zap.String("traceId", middleware.GetTraceID(c.Request.Context())))
}
