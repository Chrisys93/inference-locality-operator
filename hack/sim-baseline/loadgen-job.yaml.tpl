# Filled in by run-baseline.sh (envsubst): one Job per phase.
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
  namespace: ilo-sim
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: loadgen
          image: python:3.13.1-slim
          imagePullPolicy: IfNotPresent
          command: ["python", "-I", "/app/loadgen.py"]
          env:
            - { name: TARGET, value: "${TARGET}" }
            - { name: ARM, value: "${ARM}" }
            - { name: WORKLOAD, value: "${WORKLOAD}" }
            - { name: REQUESTS, value: "${REQUESTS}" }
            - { name: CONCURRENCY, value: "${CONCURRENCY}" }
            - { name: SEED, value: "${SEED}" }
          volumeMounts:
            - { name: app, mountPath: /app }
          resources:
            requests: { cpu: 200m, memory: 64Mi }
            limits: { memory: 256Mi }
      volumes:
        - name: app
          configMap: { name: loadgen }
