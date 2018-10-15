#!/usr/bin/env bash

set -e

kubectl --kubeconfig=$KUBECONFIG delete configmap catalog-manager-conf -n testci || true
kubectl --kubeconfig=$KUBECONFIG create configmap catalog-manager-conf -n testci --from-file=../conf/${DEPLOY_ENV}/prodBase.conf
kubectl --kubeconfig=$KUBECONFIG replace -f catalog-manager-logback.yml --force
kubectl --kubeconfig=$KUBECONFIG replace -f daf_catalog_manager_${DEPLOY_ENV}.yml -n testci --force
