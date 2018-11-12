#!/usr/bin/env bash

set -e

kubectl --kubeconfig=$KUBECONFIG delete configmap catalog-manager-conf -n testci || true
kubectl --kubeconfig=$KUBECONFIG create configmap catalog-manager-conf -n testci --from-file=../conf/${DEPLOY_ENV}/prodBase.conf
kubectl --kubeconfig=$KUBECONFIG replace -f catalog-manager-logback.yml -n testci --force
kubectl --kubeconfig=$KUBECONFIG replace -f daf-catalog-manager.yml -n testci --force
