# Deploy Kafka on K8s

<p>Prerequisite: Kubectl, Kompose<p>
<p>Run the following command to translate docker compose to deployment and service yaml<p>

```
kompose convert
```

<p>Create deployments and services<p>

```
kubectl apply -f kafdrop-service.yaml,kafka-service.yaml,zookeeper-service.yaml,kafdrop-deployment.yaml,kafka-deployment.yaml,kafka-data-persistentvolumeclaim.yaml,zookeeper-deployment.yaml,zookeeper-data-persistentvolumeclaim.yaml
```

<p>Delete deployments and services<p>

```
kubectl delete services/kafka services/kafdrop services/zookeeper deployments/kafka deployments/kafdrop deployments/zookeeper pvc/kafka-data pvc/zookeeper-data
```
