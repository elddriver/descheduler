[前置条件]
拓扑感知重调度插件是通过节点的标签来识别拓扑域，而现阶段仅910A3和910A5芯片的节点会通过ascend-device-plugin自动打上标签，普通的910A2芯片的节点仍然需要手动打上拓扑域识别标签。
不同的npu芯片对应的节点标签key分别为：
910A2：
    huawei.com/topotree.domainid
910A3：
    huawei.com/topotree.superpodid
910A5：
    huawei.com/topotree.superpodid
    huawei.com/topotree.rackid
    huawei.com/topotree.serverid
本文提供两种通过查询节点rdma网口地址打上拓扑标签的方案：
1，如果集群配置好节点免密，可直接通过ssh遍历集群所有节点，并根据rdma网口ip地址打上拓扑标签。
```
#kubectl get node -owide|awk 'NR>1 {print $6,$1}' >> /etc/hosts
for node in $(kubectl get nodes -o name | cut -d/ -f2); do
      zone=$(ssh "$node" \
        "bond=\$(ibdev2netdev | awk '/mlx5_bond/ {print \$5; exit}') && \
         [ -n \"\$bond\" ] && \
         ip -4 addr show \"\$bond\" | awk '/inet /{split(\$2,a,\".\"); print a[3]}'")
      [ -n "$zone" ] && kubectl label --overwrite node "$node" "huawei.com/topotree.domainid=$zone"
    done
```
2，如果无法配置免密，可以通过daemonset的方式在每个节点上运行一个pod，通过pod的容器来查询rdma网口ip地址并打上拓扑标签。
```
apiVersion: apps/v1
    kind: DaemonSet
    metadata:
      name: rdma-labeler
      namespace: kube-system
    spec:
      selector:
        matchLabels:
          app: rdma-labeler
      template:
        metadata:
          labels:
            app: rdma-labeler
        spec:
          hostNetwork: true
          serviceAccountName: rdma-labeler
          containers:
          - name: labeler
            image: registry.paas/cmss/ubuntu:22.04-iproute2
            command:
            - /bin/bash
            - -c
            - |
              NODE=$(hostname)
              BOND=$(ibdev2netdev | awk '/mlx5_bond/ {print $5; exit}')
              [ -z "$BOND" ] && echo "no mlx5_bond found on $NODE" && sleep 86400 && exit 0
              ZONE=$(ip -4 addr show "$BOND" | awk '/inet /{split($2,a,"."); print a[3]}')
              [ -z "$ZONE" ] && echo "no IP on $BOND" && sleep 86400 && exit 0
              kubectl label --overwrite node "$NODE" "rdma-zone=$ZONE"
              echo "labeled $NODE rdma-zone=$ZONE"
              sleep 86400
            volumeMounts:
            - name: host-sbin
              mountPath: /usr/sbin
            - name: host-bin
              mountPath: /usr/bin
          volumes:
          - name: host-sbin
            hostPath:
              path: /usr/sbin
          - name: host-bin
            hostPath:
              path: /usr/bin
          tolerations:
          - operator: Exists

    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: rdma-labeler
      namespace: kube-system

    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: rdma-labeler
    rules:
    - apiGroups: [""]
      resources: ["nodes"]
      verbs: ["get", "list", "patch"]

    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: rdma-labeler
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: rdma-labeler
    subjects:
    - kind: ServiceAccount
      name: rdma-labeler
      namespace: kube-system
```
用户也可以自行修改节点标签值来调整集群内拓扑域的分布。当前拓扑感知重调度插件针对910A2芯片的集群，通过huawei.com/topotree.domainid标签感知拓扑域，910A3/A5芯片的集群，通过huawei.com/topotree.superpodid标签感知拓扑域。
