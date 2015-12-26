# Tools

Various tools integrate with Aurora. There is a tool missing? Let us know, or submit a patch to add it!

* Load-balacing technology used to direct traffic to services running on Aurora
  - [synapse](https://github.com/airbnb/synapse) based on HAProxy
  - [aurproxy](https://github.com/tellapart/aurproxy) based on nginx
  - [jobhopper](https://github.com/benley/aurora-jobhopper) performing HTTP redirects for easy developers and administor access

* Monitoring
  - [collectd-aurora](https://github.com/zircote/collectd-aurora) for cluster monitoring using collectd
  - [Prometheus Aurora exporter](https://github.com/tommyulfsparre/aurora_exporter) for cluster monitoring using Prometheus
  - [Prometheus service discovery integration](http://prometheus.io/docs/operating/configuration/#zookeeper-serverset-sd-configurations-serverset_sd_config) for discovering and monitoring services running on Aurora

* Packaging and deployment
  - [aurora-packaging](https://github.com/apache/aurora-packaging), the source of the official Aurora packaes
