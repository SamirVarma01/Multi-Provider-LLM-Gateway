# Config is baked in, like the route tables: bind mounts are not available on
# every Docker host. Rebuild after editing prometheus.yml.
FROM prom/prometheus:v3.5.0
COPY prometheus.yml /etc/prometheus/prometheus.yml
