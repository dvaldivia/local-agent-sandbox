// Copyright 2026 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubefacade

import (
	"encoding/json"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// writeJSON encodes v as JSON with the given status code.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErr converts an error into a Kubernetes metav1.Status response so
// client-go's typed error helpers (IsNotFound, IsAlreadyExists, …) recognize it.
func writeErr(w http.ResponseWriter, err error) {
	status := metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Message:  err.Error(),
		Code:     http.StatusInternalServerError,
		Reason:   metav1.StatusReasonInternalError,
	}
	if apiStatus, ok := err.(apierrors.APIStatus); ok {
		s := apiStatus.Status()
		status.Message = s.Message
		status.Reason = s.Reason
		status.Details = s.Details
		status.Code = s.Code
	}
	writeJSON(w, int(status.Code), &status)
}

// writeStatusOK writes a Success Status (used for some delete responses).
func writeStatusOK(w http.ResponseWriter, message string) {
	writeJSON(w, http.StatusOK, &metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusSuccess,
		Message:  message,
		Code:     http.StatusOK,
	})
}
