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
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	//	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	frameworktypes "sigs.k8s.io/descheduler/pkg/framework/types"
	"sigs.k8s.io/descheduler/pkg/utils"
)

const (
	PluginName = "RemovePodsViolatingTopologyDomain"
	NPUPrefix  = "huawei.com"
)

var _ frameworktypes.DeschedulePlugin = &RemovePodsViolatingTopologyDomain{}

// RemovePodsViolatingTopologyDomain evicts pods that are not in the dominant
// topology domain for their group. For a given topology key and label selector,
// it finds the topology domain with the most pods and evicts pods in other domains.
type RemovePodsViolatingTopologyDomain struct {
	logger    klog.Logger
	handle    frameworktypes.Handle
	args      *RemovePodsViolatingTopologyDomainArgs
	podFilter podutil.FilterFunc
	selector  labels.Selector
}

// TopologyMapping 存储一个推理任务中所有实例与拓扑域的对应关系(key:value = 拓扑域:域内实例列表)
type TopologyMapping map[string][]*v1.Pod

// TaskTopology 表示一个推理任务在所有拓扑域中的分布
type TaskTopology struct {
	// OwnerKey 是任务标识，格式为 "Kind/Namespace/Name"
	// 如 "Deployment/default/llama-inference"
	OwnerKey string
	// Domains 按拓扑域分组
	Domains TopologyMapping
	// DominantDomain 是实例数量最多的拓扑域
	DominantDomain string
	// 该任务内每个 Pod 请求的 NPU 数量
	RequestNPU int64
}

// New builds plugin from its arguments while passing a handle.
func New(ctx context.Context, args runtime.Object, handle frameworktypes.Handle) (frameworktypes.Plugin, error) {
	pluginArgs, ok := args.(*RemovePodsViolatingTopologyDomainArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type RemovePodsViolatingTopologyDomainArgs, got %T", args)
	}
	logger := klog.FromContext(ctx).WithValues("plugin", PluginName)

	var includedNamespaces, excludedNamespaces sets.Set[string]
	if pluginArgs.Namespaces != nil {
		includedNamespaces = sets.New(pluginArgs.Namespaces.Include...)
		excludedNamespaces = sets.New(pluginArgs.Namespaces.Exclude...)
	}

	// Build pod filter function
	podFilter, err := podutil.NewOptions().
		WithFilter(handle.Evictor().Filter).
		WithLabelSelector(pluginArgs.LabelSelector).
		WithNamespaces(includedNamespaces).
		WithoutNamespaces(excludedNamespaces).
		WithFilter(func(pod *v1.Pod) bool { // 过滤出执行推理任务的pod
			for _, container := range pod.Spec.Containers {
				if strings.Contains(container.Image, "vllm") {
					return true
				}
			}
			return false
		}).
		BuildFilterFunc()
	if err != nil {
		return nil, fmt.Errorf("error initializing pod filter function: %v", err)
	}

	selector := labels.Everything()
	if pluginArgs.LabelSelector != nil {
		selector, err = metav1.LabelSelectorAsSelector(pluginArgs.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("error parsing label selector: %v", err)
		}
	}

	return &RemovePodsViolatingTopologyDomain{
		logger:    logger,
		handle:    handle,
		podFilter: podFilter,
		args:      pluginArgs,
		selector:  selector,
	}, nil
}

// Name retrieves the plugin name.
func (d *RemovePodsViolatingTopologyDomain) Name() string {
	return PluginName
}

// ownerKey从pod的OwnerReference中提取顶层控制器标识
// 格式为 "Kind/Namespace/Name"，如 "Deployment/default/llama-inference"
func ownerKey(pod *v1.Pod) string {
	ns := pod.Namespace
	if len(pod.OwnerReferences) == 0 {
		return fmt.Sprintf("Orphan/%s/%s", ns, pod.Name)
	}
	// 取第一个 OwnerReference 作为任务标识（通常 Deployment -> ReplicaSet -> Pod 链的顶层）
	ref := pod.OwnerReferences[0]
	return fmt.Sprintf("%s/%s/%s", ref.Kind, ns, ref.Name)
}

// PodScore 保存 Pod 的拓扑打分结果
type PodScore struct {
	Pod            *v1.Pod
	Score          int32  // 0-100，越高表示拓扑分布越优
	DominantDomain string // 该 Pod 所属任务的主拓扑域，用于驱逐前容量检查
	DomainCount    int    // 该 Pod 所在域的实例数
	DominantCount  int    // 主域的实例数
}

// resolveTopologyKey 解析实际使用的拓扑域标签键。
// 优先通过 TopologyLabelPrefix 自动发现节点上的拓扑标签，
// 未发现时回退到 TopologyKey。
func (d *RemovePodsViolatingTopologyDomain) resolveTopologyKey(nodes []*v1.Node) (string, error) {
	if d.args.TopologyLabelPrefix != "" {
		matchedLabels := sets.New[string]()
		for _, node := range nodes {
			for labelKey := range node.Labels {
				if strings.HasPrefix(labelKey, d.args.TopologyLabelPrefix) {
					suffix := strings.TrimPrefix(labelKey, d.args.TopologyLabelPrefix)
					matchedLabels.Insert(suffix)
				}
			}
		}
		if matchedLabels.Len() > 0 {
			for _, priority := range d.args.TopologyLabelPriority {
				if matchedLabels.Has(priority) {
					return d.args.TopologyLabelPrefix + priority, nil
				}
			}
			first, _ := matchedLabels.PopAny()
			return d.args.TopologyLabelPrefix + first, nil
		}
	}
	if d.args.TopologyKey == "" {
		return "", fmt.Errorf("no topology label found and topologyKey is empty")
	}
	return d.args.TopologyKey, nil
}

// scoreTaskTopologies 为所有推理任务的 Pod 打分，按分数升序返回。
// 主拓扑域中的 Pod → 100 分。
// 非主拓扑域按数值差打分：effectiveDiff = min(|id偏移 - id主|, maxEffectiveDiff)，
// score = 100 - effectiveDiff * 80 / maxEffectiveDiff，最低 20 分。
// 分数相同时，Pod 数占比更小的域优先驱逐。
func scoreTaskTopologies(taskTopologies []*TaskTopology, maxEffectiveDiff int32) []PodScore {
	var scored []PodScore
	for _, task := range taskTopologies {
		dominantCount := len(task.Domains[task.DominantDomain])
		if dominantCount == 0 {
			continue
		}
		for domain, pods := range task.Domains {
			var score int32
			if domain == task.DominantDomain {
				score = 100
			} else {
				// 尝试按数值差打分
				domID, errDom := strconv.ParseInt(task.DominantDomain, 10, 64)
				offID, errOff := strconv.ParseInt(domain, 10, 64)
				if errDom == nil && errOff == nil {
					diff := offID - domID
					if diff < 0 {
						diff = -diff
					}
					effectiveDiff := int32(diff)
					if effectiveDiff > maxEffectiveDiff {
						effectiveDiff = maxEffectiveDiff
					}
					score = 100 - effectiveDiff*80/maxEffectiveDiff
				} else {
					// 非数字 ID 时使用比例打分
					score = int32(len(pods) * 100 / dominantCount)
				}
			}
			for _, pod := range pods {
				scored = append(scored, PodScore{
					Pod:            pod,
					Score:          score,
					DominantDomain: task.DominantDomain,
					DomainCount:    len(pods),
					DominantCount:  dominantCount,
				})
			}
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score != scored[j].Score {
			return scored[i].Score < scored[j].Score
		}
		// 分数相同时，所在域 Pod 占比更小的优先驱逐
		ratioI := scored[i].DomainCount * 100 / scored[i].DominantCount
		ratioJ := scored[j].DomainCount * 100 / scored[j].DominantCount
		return ratioI < ratioJ
	})
	return scored
}

// scoreOffDomainPodsForTask 对单个推理任务的非主域 Pod 打分，按分数升序返回。
func scoreOffDomainPodsForTask(task *TaskTopology, maxEffectiveDiff int32) []PodScore {
	dominantCount := len(task.Domains[task.DominantDomain])
	if dominantCount == 0 {
		return nil
	}
	var scored []PodScore
	for domain, pods := range task.Domains {
		if domain == task.DominantDomain {
			continue
		}
		var score int32
		domID, errDom := strconv.ParseInt(task.DominantDomain, 10, 64)
		offID, errOff := strconv.ParseInt(domain, 10, 64)
		if errDom == nil && errOff == nil {
			diff := offID - domID
			if diff < 0 {
				diff = -diff
			}
			effectiveDiff := int32(diff)
			if effectiveDiff > maxEffectiveDiff {
				effectiveDiff = maxEffectiveDiff
			}
			score = 100 - effectiveDiff*80/maxEffectiveDiff
		} else {
			score = int32(len(pods) * 100 / dominantCount)
		}
		for _, pod := range pods {
			scored = append(scored, PodScore{
				Pod:            pod,
				Score:          score,
				DominantDomain: task.DominantDomain,
				DomainCount:    len(pods),
				DominantCount:  dominantCount,
			})
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score != scored[j].Score {
			return scored[i].Score < scored[j].Score
		}
		ratioI := scored[i].DomainCount * 100 / scored[i].DominantCount
		ratioJ := scored[j].DomainCount * 100 / scored[j].DominantCount
		return ratioI < ratioJ
	})
	return scored
}

// buildTaskTopologies 遍历 Pod 列表，按推理任务分组后建立拓扑域映射
// 返回 TaskTopology 切片，每个元素对应一个推理任务在各拓扑域中的分布
func (d *RemovePodsViolatingTopologyDomain) buildTaskTopologies(pods []*v1.Pod, nodeMap map[string]*v1.Node, topologyKey string, prefix string) []*TaskTopology {
	// 按推理任务分组
	taskGroups := make(map[string][]*v1.Pod)
	for _, pod := range pods {
		if utils.IsPodTerminating(pod) {
			continue
		}
		key := ownerKey(pod)
		taskGroups[key] = append(taskGroups[key], pod)
	}

	// 为每个任务构建 TaskTopology
	var taskTopologies []*TaskTopology
	for ownerKey, pods := range taskGroups {
		domains := make(TopologyMapping)
		for _, pod := range pods {
			node, ok := nodeMap[pod.Spec.NodeName]
			if !ok {
				klog.Warningf("Pod %s does not exist in nodeMap", klog.KObj(pod))
				continue
			}
			domainValue, ok := node.Labels[topologyKey]
			if !ok {
				klog.Warningf("Node %s does not have a topology label", klog.KObj(node))
				continue
			}
			domains[domainValue] = append(domains[domainValue], pod)
		}
		if len(domains) <= 1 {
			continue // 只有一个或没有拓扑域，不需要处理
		}

		// 找出主导拓扑域
		var dominantDomain string
		var maxCount int
		for domain, pods := range domains {
			if len(pods) > maxCount {
				maxCount = len(pods)
				dominantDomain = domain
			}
		}

		// 取该任务首个 Pod 的 NPU 请求数（同任务内所有 Pod 规格一致）
		requestNPU := getPodNPURequest(pods[0], prefix)

		taskTopologies = append(taskTopologies, &TaskTopology{
			Domains:        domains,
			DominantDomain: dominantDomain,
			OwnerKey:       ownerKey,
			RequestNPU:     requestNPU,
		})
	}
	return taskTopologies
}

// getPodNPURequest 返回 Pod 请求的 NPU 数量（优先 Requests，回退到 Limits）。
// 通过前缀匹配资源名（如 "huawei.com/"）以兼容不同 NPU 型号。
func getPodNPURequest(pod *v1.Pod, prefix string) int64 {
	var npuNums int64
	for _, container := range pod.Spec.Containers {
		for resourceName, qty := range container.Resources.Requests {
			if strings.HasPrefix(string(resourceName), prefix) {
				npuNums = qty.Value()
			}
		}
	}
	if npuNums == 0 {
		for _, container := range pod.Spec.Containers {
			for resourceName, qty := range container.Resources.Limits {
				if strings.HasPrefix(string(resourceName), prefix) {
					npuNums = qty.Value()
				}
			}
		}
	}
	return npuNums
}

// getNodeTotalNPU 返回节点上匹配前缀的 NPU 资源总量
func getNodeTotalNPU(node *v1.Node, prefix string) int64 {
	for resourceName, qty := range node.Status.Allocatable {
		if strings.HasPrefix(string(resourceName), prefix) {
			return qty.Value()
		}
	}
	return 0
}

// buildFreeNPUMap 构建拓扑域内各节点的空闲 NPU 映射（节点名 → 空闲卡数）
func buildFreeNPUMap(nodes []*v1.Node, getPodsAssignedToNode podutil.GetPodsAssignedToNodeFunc, prefix string) map[string]int64 {
	freeMap := make(map[string]int64, len(nodes))
	for _, node := range nodes {
		total := getNodeTotalNPU(node, prefix)
		if total == 0 {
			continue
		}
		var used int64
		pods, _ := getPodsAssignedToNode(node.Name, nil)
		for _, pod := range pods {
			used += getPodNPURequest(pod, prefix)
		}
		freeMap[node.Name] = total - used
	}
	return freeMap
}

// evictOffDomainPodsForTask 对一个推理任务的非主域 Pod 执行批量驱逐。
// 构建主域节点空闲 NPU 映射，按分数从低到高逐个检查能否容纳 Pod。
func evictOffDomainPodsForTask(ctx context.Context, handle frameworktypes.Handle, task *TaskTopology, scoredPods []PodScore, dominantNodes []*v1.Node, prefix string) {
	logger := klog.FromContext(ctx)
	getPodsAssignedToNode := handle.GetPodsAssignedToNodeFunc()

	// 构建主域节点空闲 NPU 映射
	freeMap := buildFreeNPUMap(dominantNodes, getPodsAssignedToNode, prefix)
	logger.V(3).Info("Dominant domain free NPU map",
		"dominantDomain", task.DominantDomain,
		"freeNPUMap", freeMap)

	for _, ps := range scoredPods {
		// 逐个查找空闲 NPU 足够的节点
		targetNode := ""
		for nodeName, free := range freeMap {
			if free >= task.RequestNPU {
				targetNode = nodeName
				break
			}
		}
		if targetNode == "" {
			logger.V(2).Info("Skipping pod eviction: no node in dominant domain has enough free NPUs",
				"pod", klog.KObj(ps.Pod),
				"dominantDomain", task.DominantDomain,
				"requiredNPUs", task.RequestNPU)
			continue
		}

		logger.V(1).Info("Evicting pod with low topology score",
			"pod", klog.KObj(ps.Pod),
			"task", ownerKey(ps.Pod),
			"score", ps.Score,
			"dominantDomain", task.DominantDomain,
			"targetNode", targetNode)
		//		err := handle.Evictor().Evict(ctx, ps.Pod, evictions.EvictOptions{StrategyName: PluginName})
		//		if err != nil {
		//			logger.Error(err, "failed to evict pod", "pod", klog.KObj(ps.Pod))
		//		} else {
		//			freeMap[targetNode] -= task.RequestNPU
		//		}
		freeMap[targetNode] -= task.RequestNPU
	}
}

// Deschedule 遍历所有推理任务，对每个 Pod 进行拓扑打分，
// 优先驱逐分数低（偏移主拓扑域）的 Pod。
func (d *RemovePodsViolatingTopologyDomain) Deschedule(ctx context.Context, nodes []*v1.Node) *frameworktypes.Status {
	logger := klog.FromContext(klog.NewContext(ctx, d.logger)).WithValues("ExtensionPoint", frameworktypes.DescheduleExtensionPoint)
	logger.V(1).Info("Processing pods for topology domain violation")

	// 解析拓扑域标签键
	topologyKey, err := d.resolveTopologyKey(nodes)
	if err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error resolving topology key: %v", err),
		}
	}

	nodeMap := make(map[string]*v1.Node, len(nodes))
	domainNodes := make(map[string][]*v1.Node)
	for _, node := range nodes {
		nodeMap[node.Name] = node
		domain, ok := node.Labels[topologyKey]
		if ok {
			domainNodes[domain] = append(domainNodes[domain], node)
		}
	}

	pods, err := podutil.ListPodsOnNodes(nodes, d.handle.GetPodsAssignedToNodeFunc(), d.podFilter)
	if err != nil {
		return &frameworktypes.Status{
			Err: fmt.Errorf("error listing pods: %v", err),
		}
	}

	taskTopologies := d.buildTaskTopologies(pods, nodeMap, topologyKey, NPUPrefix)
	if len(taskTopologies) == 0 {
		logger.V(2).Info("No inference tasks with multi-domain distribution found")
		return nil
	}

	for _, task := range taskTopologies {
		scoredPods := scoreOffDomainPodsForTask(task, *d.args.MaxEffectiveDiff)
		if len(scoredPods) == 0 {
			continue
		}
		logger.V(4).Info("Task topology scores",
			"task", task.OwnerKey,
			"podCount", len(scoredPods))
		for _, ps := range scoredPods {
			logger.V(4).Info("Topology score",
				"pod", klog.KObj(ps.Pod),
				"domain", ps.DominantDomain,
				"score", ps.Score)
		}

		dominantNodes := domainNodes[task.DominantDomain]
		evictOffDomainPodsForTask(ctx, d.handle, task, scoredPods, dominantNodes, NPUPrefix)
	}

	return nil
}
