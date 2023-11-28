#!/bin/bash

    IFS=
    SRV=$(cat <<EOF
[Unit]
Description=Prometheus Node Exporter Service
After=network.target

[Service]
User=node_exporter
Group=node_exporter
Type=simple
ExecStart=/usr/local/bin/node_exporter $OPTIONS
Restart=always

[Install]
WantedBy=multi-user.target
EOF
       )
    echo $SRV | tee /etc/systemd/system/node-exporter.service  > /dev/null

systemctl daemon-reload
systemctl enable node-exporter
systemctl start node-exporter
systemctl status node-exporter
