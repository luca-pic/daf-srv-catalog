#!/usr/bin/env bash

set -e

kubectl --kubeconfig=$KUBECONFIG delete configmap catalog-manager-conf || true
kubectl --kubeconfig=$KUBECONFIG create configmap catalog-manager-conf --from-file=../conf/${DEPLOY_ENV}/prodBase.conf
kubectl --kubeconfig=$KUBECONFIG replace -f catalog-manager-logback.yml --force
kubectl --kubeconfig=$KUBECONFIG replace -f daf-catalog-manager.yml --force
