# Deploy Kafka on K8s

<p>Prerequisite: Kubectl, Kompose<p>
<p>Run the following command to translate docker compose to deployment and service yaml<p>

```
kompose convert
```

<p>Create deployments and services<p>

```
kubectl apply -f zookeeper-deployment.yaml,zookeeper-service.yaml,zookeeper-data-persistentvolumeclaim.yaml,kafka-deployment.yaml,kafka-service.yaml,kafka-data-persistentvolumeclaim.yaml
```

<p>Delete deployments and services<p>

```
kubectl delete services/kafka services/zookeeper deployments/kafka deployments/zookeeper pvc/kafka-data pvc/zookeeper-data
```
