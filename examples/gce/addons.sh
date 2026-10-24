#!/usr/bin/env bash
# Copyright 2026 The Brie Authors.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077
export KUBECONFIG=/etc/kubernetes/admin.conf
work=/var/lib/brie-experiment/addons
mkdir -p "$work"
cd "$work"
fetch() {
  local url="$1" file="$2" digest="$3"
  curl --fail --silent --show-error --location --retry 3 "$url" -o "$file"
  printf '%s  %s\n' "$digest" "$file" | sha256sum --check --strict
}
fetch https://get.helm.sh/helm-v4.2.2-linux-amd64.tar.gz helm.tgz \
  9adafecab4d406853bba163a70e9f104f47dbbf65ce24b7653bae7e36150bcb6
tar -xzf helm.tgz linux-amd64/helm
fetch https://helm.cilium.io/cilium-1.20.4.tgz cilium.tgz \
  505abff8385811df1a949776fc42e9f10697989a332acc86d6e8933efcb855ed
./linux-amd64/helm template cilium cilium.tgz --namespace kube-system \
  --set kubeProxyReplacement=false --set ipam.mode=kubernetes \
  --set operator.replicas=1 --set operator.tolerations[0].operator=Exists \
  --set routingMode=tunnel --set tunnelProtocol=vxlan > cilium.yaml
kubectl apply -f cilium.yaml
kubectl -n kube-system rollout status daemonset/cilium --timeout=300s
kubectl -n kube-system rollout status deployment/cilium-operator --timeout=180s
kubectl wait --for=condition=Ready nodes --all --timeout=180s

fetch https://github.com/kubernetes-sigs/node-feature-discovery/releases/download/v0.20.1/node-feature-discovery-chart-0.20.1.tgz nfd.tgz \
  c7e2a38ca3bf0faca5ec7ce0981eeb24bf249407748b35a4b6f087f5a8c7acdd
./linux-amd64/helm template nfd nfd.tgz --namespace node-feature-discovery \
  --include-crds --no-hooks --set worker.tolerations[0].operator=Exists \
  --set gc.tolerations[0].operator=Exists > nfd.yaml
kubectl create namespace node-feature-discovery --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f nfd.yaml

fetch https://raw.githubusercontent.com/kubernetes/node-problem-detector/v1.37.0/deployment/node-problem-detector-config.yaml npd-config.yaml \
  c83564570cd8143647854f3f33fd1f78120516d550eced9ebfd3f0e87ff4b6b9
kubectl apply -f npd-config.yaml
# Only kernel/kmsg monitoring: the host uses containerd, not Docker/syslog.
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: node-problem-detector
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: brie-node-problem-detector
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:node-problem-detector
subjects:
  - kind: ServiceAccount
    name: node-problem-detector
    namespace: kube-system
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: node-problem-detector
  namespace: kube-system
spec:
  selector:
    matchLabels: {app: node-problem-detector}
  template:
    metadata:
      labels: {app: node-problem-detector}
    spec:
      serviceAccountName: node-problem-detector
      nodeSelector: {kubernetes.io/os: linux}
      tolerations:
        - operator: Exists
      containers:
        - name: node-problem-detector
          image: registry.k8s.io/node-problem-detector/node-problem-detector:v0.8.19@sha256:a28d0bd3f6ecc9648ecedd7135c31c31751e340017cc9a663911f7b064a8a010
          command:
            - /node-problem-detector
            - --logtostderr
            - --config.system-log-monitor=/config/kernel-monitor.json
          env:
            - name: NODE_NAME
              valueFrom: {fieldRef: {fieldPath: spec.nodeName}}
          resources:
            requests: {cpu: 10m, memory: 80Mi}
            limits: {memory: 160Mi}
          securityContext: {privileged: true}
          volumeMounts:
            - {name: kmsg, mountPath: /dev/kmsg, readOnly: true}
            - {name: config, mountPath: /config, readOnly: true}
      volumes:
        - name: kmsg
          hostPath: {path: /dev/kmsg, type: CharDevice}
        - name: config
          configMap: {name: node-problem-detector-config}
EOF

# Approve only the initial serving CSR of this one known kubeadm node.
# Do not install a permissive ongoing CSR approver.
node="$(kubectl get nodes -o json | jq -er 'if (.items | length) == 1 then .items[0].metadata.name else error("expected one upstream node") end')"
ip="$(kubectl get node "$node" -o json | jq -er '.status.addresses[] | select(.type == "InternalIP") | .address')"
csr=""
for ((attempt=0; attempt<60; attempt++)); do
  csr="$(kubectl get csr -o json | jq -r --arg user "system:node:$node" \
    '[.items[] | select(.spec.signerName == "kubernetes.io/kubelet-serving" and .spec.username == $user and (all(.status.conditions[]?; .type != "Denied" and .type != "Failed")))] | sort_by(.metadata.creationTimestamp, .metadata.name) | last | .metadata.name // empty')"
  [[ -z "$csr" ]] || break
  sleep 2
done
[[ -n "$csr" && "$csr" != *$'\n'* ]] || { echo 'Expected exactly one node serving CSR' >&2; exit 1; }
kubectl get csr "$csr" -o json > serving-csr.json
jq -e --arg user "system:node:$node" '
  .spec.signerName == "kubernetes.io/kubelet-serving" and .spec.username == $user
  and (((.spec.usages | sort) == (["digital signature","server auth"] | sort))
    or ((.spec.usages | sort) == (["digital signature","key encipherment","server auth"] | sort)))
  and (.spec.groups | index("system:nodes") != null)
' serving-csr.json >/dev/null
jq -r '.spec.request' serving-csr.json | base64 -d > serving.csr
openssl req -in serving.csr -noout -verify
subject="$(openssl req -in serving.csr -noout -subject -nameopt RFC2253)"
[[ "$subject" == "subject=CN=system:node:$node,O=system:nodes" ]] || { echo 'Unexpected CSR subject' >&2; exit 1; }
openssl req -in serving.csr -noout -text > serving-csr.txt
san="$(sed -n '/X509v3 Subject Alternative Name:/{n;s/^[[:space:]]*//;p;}' serving-csr.txt)"
[[ "$san" == "DNS:$node, IP Address:$ip" ]] || { echo 'Unexpected CSR SANs' >&2; exit 1; }
kubectl certificate approve "$csr"

fetch https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml metrics.yaml \
  1cec29a5267809306a2c6ec74a3e449abbb705b4a8beed0c8a1963910f72c79b
# Trust the kubeadm CA for kubelet serving TLS; never use --kubelet-insecure-tls.
kubectl apply -f metrics.yaml
kubectl -n kube-system patch deployment metrics-server --type=strategic \
  --patch='{"spec":{"template":{"spec":{"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}],"containers":[{"name":"metrics-server","args":["--cert-dir=/tmp","--secure-port=10250","--kubelet-preferred-address-types=InternalIP","--kubelet-use-node-status-port","--metric-resolution=15s","--kubelet-certificate-authority=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"]}]}}}}' \
  -o yaml > metrics-patched.yaml
kubectl -n kube-system rollout status deployment/metrics-server --timeout=240s
kubectl -n kube-system rollout status daemonset/node-problem-detector --timeout=180s
kubectl -n node-feature-discovery rollout status deployment/nfd-node-feature-discovery-master --timeout=180s
kubectl -n node-feature-discovery rollout status daemonset/nfd-node-feature-discovery-worker --timeout=180s
kubectl wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=180s
kubectl top nodes
kubectl create -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: brie-network-smoke
  namespace: default
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  tolerations:
    - key: node-role.kubernetes.io/control-plane
      operator: Exists
      effect: NoSchedule
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: dns
      image: registry.k8s.io/e2e-test-images/busybox:1.36.1-1@sha256:a9155b13325b2abef48e71de77bb8ac015412a566829f621d06bfae5c699b1b9
      command: [sh, -ec, "nslookup kubernetes.default.svc.cluster.local"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: {drop: [ALL]}
      resources:
        requests: {cpu: 10m, memory: 16Mi}
        limits: {cpu: 100m, memory: 32Mi}
EOF
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/brie-network-smoke --timeout=120s
kubectl logs brie-network-smoke
kubectl delete pod brie-network-smoke --wait=true
