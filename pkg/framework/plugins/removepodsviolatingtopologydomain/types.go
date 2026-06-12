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

	// TopologyKey 显式指定拓扑域标签键。
	// 如果 TopologyLabelPrefix 未匹配到任何标签，则使用此值作为拓扑域划分依据。
	// 默认 "huawei.com/topotree.domainid"。
	TopologyKey string `json:"topologyKey,omitempty"`

	// TopologyLabelPrefix 自动发现拓扑域标签的前缀。
	// 插件扫描节点标签，从匹配此前缀的标签中按 LabelPriority 顺序选择。
	// 例如 "huawei.com/topotree." 会匹配 huawei.com/topotree.superpodid 等标签。
	TopologyLabelPrefix string `json:"topologyLabelPrefix,omitempty"`

	// TopologyLabelPriority 自动发现时按此顺序选择拓扑标签后缀。
	// 例如 ["superpodid", "domainid"] 表示优先使用 superpodid。
	TopologyLabelPriority []string `json:"topologyLabelPriority,omitempty"`

	// MaxEffectiveDiff 最大有效差值。
	// 拓扑域 ID 的差值超过此值时分数不再继续降低，
	// 即所有超过阈值的偏移域视为同等偏移。默认 5。
	// +optional
	MaxEffectiveDiff *int32 `json:"maxEffectiveDiff,omitempty"`

	// NPUResourceName 节点上 NPU 资源名，用于计算主域剩余容量。
	// 默认 "huawei.com/Ascend910"。
	// +optional
	NPUResourceName *string `json:"npuResourceName,omitempty"`

	// LabelSelector is used to identify pods belonging to the same group
	// (e.g., an inference deployment). The plugin will find the dominant
	// topology domain for this group and evict pods outside of it.
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// Namespaces allows filtering on which namespaces to apply the descheduler.
	Namespaces *api.Namespaces `json:"namespaces,omitempty"`
}
