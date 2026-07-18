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

// Package vrouter materializes the virtual Kubernetes objects that the
// agent-sandbox SDKs and kubectl use to locate the router before
// port-forwarding: a Pod (sandbox-router-0), a headless Service
// (sandbox-router-svc), and an EndpointSlice pointing at the pod. These are
// inert store records; the only behavior behind them is the portforward
// subresource served by internal/portforward.
package vrouter

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

const (
	PodName     = "sandbox-router-0"
	ServiceName = "sandbox-router-svc"
	// SystemNamespace is kubectl's default router namespace (python SDK).
	SystemNamespace = "agent-sandbox-system"

	appLabel   = "app"
	appValue   = "sandbox-router"
	routerPort = 8080
)

func toU(obj any) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: m}, nil
}

// EnsureNamespace materializes the router Pod/Service/EndpointSlice in ns if
// they are absent. Safe to call repeatedly.
func EnsureNamespace(st *store.Store, ns string) {
	ensurePod(st, ns)
	ensureService(st, ns)
	ensureEndpointSlice(st, ns)
}

func ensurePod(st *store.Store, ns string) {
	if _, err := st.Get(apis.PodGVR, ns, PodName); err == nil {
		return
	}
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: PodName, Namespace: ns, Labels: map[string]string{appLabel: appValue}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "router", Image: "lasd/router:virtual"}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{ready},
			PodIP:      "127.0.0.1",
			PodIPs:     []corev1.PodIP{{IP: "127.0.0.1"}},
		},
	}
	if u, err := toU(pod); err == nil {
		create(st, apis.PodGVR, u)
	}
}

func ensureService(st *store.Store, ns string) {
	if _, err := st.Get(apis.ServiceGVR, ns, ServiceName); err == nil {
		return
	}
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: ServiceName, Namespace: ns, Labels: map[string]string{appLabel: appValue}},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  map[string]string{appLabel: appValue},
			Ports:     []corev1.ServicePort{{Name: "http", Port: routerPort, TargetPort: intstr.FromInt32(routerPort)}},
		},
	}
	if u, err := toU(svc); err == nil {
		create(st, apis.ServiceGVR, u)
	}
}

func ensureEndpointSlice(st *store.Store, ns string) {
	name := ServiceName + "-1"
	if _, err := st.Get(apis.EndpointSliceGVR, ns, name); err == nil {
		return
	}
	ready := true
	port := int32(routerPort)
	portName := "http"
	eps := &discoveryv1.EndpointSlice{
		TypeMeta: metav1.TypeMeta{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns,
			Labels: map[string]string{"kubernetes.io/service-name": ServiceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"127.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: ns, Name: PodName},
		}},
		Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port}},
	}
	if u, err := toU(eps); err == nil {
		create(st, apis.EndpointSliceGVR, u)
	}
}

func create(st *store.Store, gvr apis.GVR, u *unstructured.Unstructured) {
	if _, err := st.Create(gvr, u); err != nil && !apierrors.IsAlreadyExists(err) {
		// best-effort; virtual objects are non-critical to control-plane CRUD
		_ = err
	}
}
