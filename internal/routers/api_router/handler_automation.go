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

// ListExecutions returns execution records for the current user.
// @Summary List automation executions
// @Tags Automation
// @Security UserAuthToken
// @Produce json
// @Param params query dto.AutomationExecutionListRequest true "Automation execution filters"
// @Success 200 {object} pkgapp.Res{data=pkgapp.ListRes{list=[]dto.AutomationExecutionDTO}} "Success"
// @Router /api/automations/executions [get]
func (h *AutomationHandler) ListExecutions(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	params := &dto.AutomationExecutionListRequest{}
	pager := pkgapp.NewPager(c)
	if valid, errs := pkgapp.BindAndValid(c, params); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	items, total, err := h.App.AutomationService.ListExecutions(c.Request.Context(), uid, params.TriggerID, pager.Page, pager.PageSize)
	if err != nil {
		h.logError(c, "AutomationHandler.ListExecutions", err)
		apperrors.ErrorResponse(c, err)
		return
	}
	response.ToResponseList(code.Success, items, int(total))
}

// RetryExecution retries only actions that did not succeed in an execution.
// @Summary Retry automation execution
// @Tags Automation
// @Security UserAuthToken
// @Accept json
// @Produce json
// @Param request body dto.AutomationExecutionRetryRequest true "Execution ID"
// @Success 200 {object} pkgapp.Res "Success"
// @Router /api/automations/executions/retry [post]
func (h *AutomationHandler) RetryExecution(c *gin.Context) {
	response := pkgapp.NewResponse(c)
	request := &dto.AutomationExecutionRetryRequest{}
	if valid, errs := pkgapp.BindAndValid(c, request); !valid {
		response.ToResponse(code.ErrorInvalidParams.WithDetails(errs.ErrorsToString()).WithData(errs.MapsToString()))
		return
	}
	uid := pkgapp.GetUID(c)
	if uid == 0 {
		response.ToResponse(code.ErrorNotUserAuthToken)
		return
	}
	if err := h.App.AutomationService.RetryExecution(c.Request.Context(), uid, request.ID); err != nil {
		h.logError(c, "AutomationHandler.RetryExecution", err)
		response.ToResponse(code.ErrorInvalidParams.WithDetails(err.Error()))
		return
	}
	response.ToResponse(code.Success.WithDetails("Automation execution retried"))
}

func (h *AutomationHandler) logError(c *gin.Context, method string, err error) {
	h.App.Logger().Warn(method, zap.Error(err), zap.String("traceId", middleware.GetTraceID(c.Request.Context())))
}
