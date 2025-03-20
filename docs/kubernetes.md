# Kubernetes Deployment Guide

This guide explains how to deploy EVM-trackooor in Kubernetes with proper health checks.

## Health Check Implementation

EVM-trackooor now includes a health check HTTP server that can be used by Kubernetes to determine if the service is healthy. The health check server provides the following endpoints:

- `/healthz` - General health check endpoint (returns HTTP 200 OK if the service is running)
- `/livez` - Liveness probe endpoint (returns HTTP 200 OK if the service is running)
- `/readyz` - Readiness probe endpoint (returns HTTP 200 OK if the service is ready to handle requests)

The readiness probe checks if the service is connected to the RPC endpoint and can fetch the latest block number, which ensures that the service can actually perform its core functionality.

## How to Enable Health Checks

To enable the health check server, pass the `--health-port` flag when running the application:

```bash
./evm-trackooor track realtime --config ./config.json --verbose --health-port 8080
```

In the Dockerfile, the health check port is exposed on port 8080 by default, but you can change this to any port you prefer.

## Kubernetes Configuration

A sample Kubernetes deployment configuration is provided in `k8s-deployment.yaml`. It includes:

1. A Deployment with liveness and readiness probes
2. A ConfigMap for your configuration
3. A Service to expose the health check endpoints
4. Resource requests and limits for CPU and memory

### Liveness Probe

The liveness probe checks if the application is running:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: health
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Readiness Probe

The readiness probe checks if the application is ready to handle requests:

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: health
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3
```

### Resource Management

Resources are defined with both requests and limits to ensure the application has sufficient resources while preventing it from consuming excessive cluster resources:

```yaml
resources:
  requests:
    cpu: "500m"        # Request 0.5 CPU cores
    memory: "512Mi"    # Request 512 MB of memory
  limits:
    cpu: "1000m"       # Limit to 1 CPU core
    memory: "1Gi"      # Limit to 1 GB of memory
```

You should adjust these values based on the actual resource needs of your specific deployment. Monitor your application's resource usage and adjust accordingly.

## Customizing the Deployment

1. Update the `config.json` in the ConfigMap with your actual configuration
2. Adjust probe timing parameters based on your application's startup time and required responsiveness
3. Set the proper image name and image pull policy
4. Adjust resource requests and limits based on your application's needs

## Deploying to Kubernetes

```bash
kubectl apply -f k8s-deployment.yaml
```

## Verifying the Deployment

```bash
# Check the deployment status
kubectl get deployment evm-trackooor

# Check the pod status
kubectl get pods -l app=evm-trackooor

# Check the pod's health check endpoints
kubectl port-forward deployment/evm-trackooor 8080:8080
curl http://localhost:8080/healthz
curl http://localhost:8080/livez
curl http://localhost:8080/readyz
```
