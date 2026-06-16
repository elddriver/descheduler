# 拓扑感知重调度插件

## 前置条件

拓扑感知重调度插件通过**节点的标签**来识别拓扑域。

- **910A3 / 910A5 芯片**：节点由 `ascend-device-plugin` 自动打上拓扑标签
- **910A2 芯片**：需要手动为节点打上拓扑域识别标签

### 不同 NPU 芯片对应的标签 Key

| NPU 类型 | 节点标签 Key |
|----------|-------------|
| 910A2 | `huawei.com/topotree.domainid` |
| 910A3 | `huawei.com/topotree.superpodid` |
| 910A5 | `huawei.com/topotree.superpodid`<br>`huawei.com/topotree.rackid`<br>`huawei.com/topotree.serverid` |

### 手动打标签（910B 集群）

通过查询节点 RDMA 网口地址来打上拓扑标签。

> 前置条件：集群已配置好节点免密登录。

```bash
# 1，将节点与IP 写入 hosts
kubectl get node -owide | awk 'NR>1 {print $6,$1}' >> /etc/hosts

# 2，遍历集群节点，根据 RDMA 网口 IP 打拓扑标签
kubectl get nodes -o name | sed 's|node/||' | while read node; do
    zone=$(ssh -n -o ConnectTimeout=5 "$node" '
        b=$(ibdev2netdev 2>/dev/null | awk "/mlx5_bond/{print\$5;exit}")
        [ -z "$b" ] && exit 1
        ip -4 addr show "$b" 2>/dev/null | awk "/inet /{split(\$2,a,\"/\");split(a[1],b,\".\");print b[3]}"
    ' 2>/dev/null) && kubectl label --overwrite node "$node" "huawei.com/topotree.domainid=$zone" && echo "$node -> zone=$zone" || echo "$node -> skipped"
done
```

> 用户也可以直接通过 `kubectl label` 手动修改节点标签值来调整集群内拓扑域的分布。

### 插件标签识别策略

| 集群类型 | 使用的标签 |
|---------|-----------|
| 910A2 | `huawei.com/topotree.domainid` |
| 910A3 / A5 | `huawei.com/topotree.superpodid` |

---

## 部署

### 1. 部署 ConfigMap（启用插件）

```bash
kubectl apply -f descheduler-policy-configmap.yaml
```

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: descheduler-policy-configmap
  namespace: kube-system
data:
  policy.yaml: |
    apiVersion: "descheduler/v1alpha2"
    kind: "DeschedulerPolicy"
    profiles:
      - name: TopologyDomainProfile
        pluginConfig:
        - name: "DefaultEvictor"
        - name: "RemovePodsViolatingTopologyDomain"
          args:
            topologyKey: "huawei.com/topotree.domainid"  # 插件感知拓扑域所用的标签
            npuResourcePrefix: "huawei.com"              # 插件通过前缀识别节点资源
            # inferencePodLabelKey: "task-type"          # 推理 Pod 识别标签 Key
            # inferencePodLabelValue: "inference"        # 推理 Pod 识别标签 Value
        plugins:
          deschedule:
            enabled:
              - "RemovePodsViolatingTopologyDomain"
```

### 2. 部署 Descheduler

```bash
kubectl apply -f descheduler.yaml
```

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: descheduler
  namespace: kube-system
  labels:
    app: descheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      app: descheduler
  template:
    metadata:
      labels:
        app: descheduler
    spec:
      priorityClassName: system-cluster-critical
      serviceAccountName: descheduler-sa
      containers:
        - name: descheduler
          image: registry.k8s.io/descheduler/descheduler:v0.35.0
          imagePullPolicy: IfNotPresent
          command:
            - "/bin/descheduler"
          args:
            - "--policy-config-file"
            - "/policy-dir/policy.yaml"
            - "--descheduling-interval"
            - "5m"
            - "--v"
            - "3"
          ports:
          - containerPort: 10258
            protocol: TCP
          livenessProbe:
            failureThreshold: 3
            httpGet:
              path: /healthz
              port: 10258
              scheme: HTTPS
            initialDelaySeconds: 3
            periodSeconds: 10
          resources:
            requests:
              cpu: 500m
              memory: 256Mi
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
            privileged: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
          volumeMounts:
            - mountPath: /policy-dir
              name: policy-volume
      volumes:
      - name: policy-volume
        configMap:
          name: descheduler-policy-configmap
```

### 3. 部署 RBAC 权限配置

```bash
kubectl apply -f rbac.yaml
```

```yaml
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: descheduler-cluster-role
rules:
- apiGroups: ["events.k8s.io"]
  resources: ["events"]
  verbs: ["create", "update"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "watch", "list"]
- apiGroups: [""]
  resources: ["namespaces"]
  verbs: ["get", "watch", "list"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "watch", "list", "delete"]
- apiGroups: [""]
  resources: ["pods/eviction"]
  verbs: ["create"]
- apiGroups: ["scheduling.k8s.io"]
  resources: ["priorityclasses"]
  verbs: ["get", "watch", "list"]
- apiGroups: ["policy"]
  resources: ["poddisruptionbudgets"]
  verbs: ["get", "watch", "list"]
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["create", "update"]
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  resourceNames: ["descheduler"]
  verbs: ["get", "patch", "delete"]
- apiGroups: ["metrics.k8s.io"]
  resources: ["nodes", "pods"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["get", "watch", "list"]
---
kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: descheduler-role
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: descheduler-sa
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: descheduler-cluster-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: descheduler-cluster-role
subjects:
  - name: descheduler-sa
    kind: ServiceAccount
    namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: descheduler-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: descheduler-role
subjects:
  - name: descheduler-sa
    kind: ServiceAccount
    namespace: kube-system
```

---

## 编译构建

1. 获取社区 descheduler 源码：<https://github.com/kubernetes-sigs/descheduler>
2. 将 `RemovePodsViolatingTopologyDomain` 目录放到源码的 `pkg/framework/plugins/` 目录下
3. 编译：
   ```bash
   make build                    # 默认编译
   make build.amd64              # 指定 amd64 架构
   ```
   编译后的文件在 `_output/bin/` 目录下
4. 打包镜像：
   ```bash
   docker build -t [镜像名:标签] -f Dockerfile.dev .
   ```
