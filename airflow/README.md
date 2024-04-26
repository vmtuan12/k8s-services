# Airflow on K8s

<p>Command to run<p>

```
helm upgrade --install airflow apache-airflow/airflow -f values_cur.yaml --namespace airflow --set dags.gitSync.env[0].value='<git-username>' --set dags.gitSync.env[1].value='<git-token>'
```

<p>Create secret that contains key for web server<p>

```
kubectl create secret generic web-server-key --from-literal="webserver-secret-key=$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
```