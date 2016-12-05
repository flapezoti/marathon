/*
 * Copyright (c) 2016 TFG Co <backend@tfgco.com>
 * Author: TFG Co <backend@tfgco.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package api

import (
	"net/http"
	"strings"

	"github.com/labstack/echo"
	"github.com/satori/go.uuid"
	"github.com/topfreegames/marathon/model"
)

// ListJobsHandler is the method called when a get to /apps/:id/templates/:tid/jobs is called
func (a *Application) ListJobsHandler(c echo.Context) error {
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	tid, err := uuid.FromString(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	jobs := []model.Job{}
	where := map[string]interface{}{
		"AppID":      id,
		"TemplateID": tid,
	}
	if err := a.DB.Where(where).Find(&jobs).Error; err != nil {
		return c.JSON(http.StatusInternalServerError, &Error{Reason: err.Error()})
	}
	return c.JSON(http.StatusOK, jobs)
}

// PostJobHandler is the method called when a post to /apps/:id/templates/:tid/jobs is called
func (a *Application) PostJobHandler(c echo.Context) error {
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	tid, err := uuid.FromString(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	email := c.Get("user-email").(string)
	job := &model.Job{
		AppID:      id,
		TemplateID: tid,
		CreatedBy:  email,
	}
	err = decodeAndValidate(c, job)
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error(), Value: job})
	}
	if err = a.DB.Create(&job).Error; err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return c.JSON(http.StatusConflict, job)
		}
		return c.JSON(http.StatusInternalServerError, &Error{Reason: err.Error(), Value: job})
	}
	return c.JSON(http.StatusCreated, job)
}

// GetJobHandler is the method called when a get to /apps/:id/templates/:tid/jobs/:jid is called
func (a *Application) GetJobHandler(c echo.Context) error {
	id, err := uuid.FromString(c.Param("id"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	tid, err := uuid.FromString(c.Param("tid"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	jid, err := uuid.FromString(c.Param("jid"))
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, &Error{Reason: err.Error()})
	}
	job := &model.Job{
		ID:         jid,
		AppID:      id,
		TemplateID: tid,
	}
	if err := a.DB.Where(job).First(&job).Error; err != nil {
		if err.Error() == RecordNotFoundString {
			return c.JSON(http.StatusNotFound, job)
		}
		return c.JSON(http.StatusInternalServerError, &Error{Reason: err.Error(), Value: job})
	}
	return c.JSON(http.StatusOK, job)
}