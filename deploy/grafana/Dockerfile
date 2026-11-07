# Provisioning is baked in, like the route tables: bind mounts are not
# available on every Docker host. Rebuild after editing anything here.
# Dashboards stay out of /var/lib/grafana: that is a volume, and a stale copy
# there would hide every later image.
FROM grafana/grafana:12.1.0
COPY provisioning/ /etc/grafana/provisioning/
COPY dashboards/ /etc/grafana/dashboards/
