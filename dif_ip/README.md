# Deploy Kafka on K8s

<p>Prerequisite: Kubectl, Kompose<p>
<p>Run the following command to translate docker compose to deployment and service yaml<p>

```
kompose convert
```

<p>Create deployments and services<p>

```
kubectl apply -f kafka-deployment.yaml,zookeeper-deployment.yaml,kafka-service.yaml,zookeeper-service.yaml,kafka-data-persistentvolumeclaim.yaml,zookeeper-data-persistentvolumeclaim.yaml
```

<p>Delete deployments and services<p>

```
kubectl delete services/kafka services/zookeeper deployments/kafka deployments/zookeeper pvc/kafka-data pvc/zookeeper-data
```
