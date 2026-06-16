/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package removepodsviolatingtopologydomain

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/descheduler/pkg/api"
)

// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RemovePodsViolatingTopologyDomainArgs holds arguments used to configure RemovePodsViolatingTopologyDomain plugin.
type RemovePodsViolatingTopologyDomainArgs struct {
	metav1.TypeMeta `json:",inline"`

	// TopologyKey 指定节点标签的键用于划分拓扑域。
	// 例如 "huawei.com/topotree.superpodid" 或 "huawei.com/topotree.domainid"。
	// 默认 "huawei.com/topotree.domainid"。
	TopologyKey string `json:"topologyKey,omitempty"`

	// MaxEffectiveDiff 最大有效差值。
	// 拓扑域 ID 的差值超过此值时分数不再继续降低，
	// 即所有超过阈值的偏移域视为同等偏移。默认 5。
	// +optional
	MaxEffectiveDiff *int32 `json:"maxEffectiveDiff,omitempty"`

	// NPUResourcePrefix 节点上 NPU 资源的前缀，用于匹配 NPU 资源名。
	// 例如 "huawei.com/" 会匹配 huawei.com/Ascend910。默认 "huawei.com"。
	// +optional
	NPUResourcePrefix string `json:"npuResourcePrefix,omitempty"`

	// LabelSelector is used to identify pods belonging to the same group
	// (e.g., an inference deployment). The plugin will find the dominant
	// topology domain for this group and evict pods outside of it.
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// InferencePodLabelKey 标识推理 Pod 的标签键。
	// 插件只处理匹配此标签的 Pod，默认 "task-type"。
	// +optional
	InferencePodLabelKey string `json:"inferencePodLabelKey,omitempty"`

	// InferencePodLabelValue 标识推理 Pod 的标签值。
	// 插件只处理匹配此标签的 Pod，默认 "inference"。
	// +optional
	InferencePodLabelValue string `json:"inferencePodLabelValue,omitempty"`

	// Namespaces allows filtering on which namespaces to apply the descheduler.
	Namespaces *api.Namespaces `json:"namespaces,omitempty"`
}
