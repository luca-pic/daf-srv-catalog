#!/usr/bin/env bash

set -e

dhall-to-yaml --omitNull <<< ./service.dhall > daf-catalog-manager.yml
echo --- >> daf-catalog-manager.yml
dhall-to-yaml --omitNull <<< ./deployment.dhall >> daf-catalog-manager.yml

kubectl --kubeconfig=$KUBECONFIG delete configmap catalog-manager-conf || true
kubectl --kubeconfig=$KUBECONFIG create configmap catalog-manager-conf --from-file=../conf/${DEPLOY_ENV}/prodBase.conf
kubectl --kubeconfig=$KUBECONFIG delete -f catalog-manager-logback.yml || true
kubectl --kubeconfig=$KUBECONFIG create -f catalog-manager-logback.yml
kubectl --kubeconfig=$KUBECONFIG delete -f daf-catalog-manager.yml || true
kubectl --kubeconfig=$KUBECONFIG create -f daf-catalog-manager.yml
