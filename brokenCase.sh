#!/bin/sh

python -m batch_sim generate --config configs/jch_centroids_v01.yaml --output workloads/jch_all_events_01.json

python scripts/generate_node_timelines.py --scheduler k8s --events workloads/jch_all_events_01.json --registry configs/jch_instance_registry.yaml --output results_new/k8s-run03 --seed 3816
python scripts/generate_node_timelines.py --scheduler batch --events workloads/jch_all_events_01.json --registry configs/jch_instance_registry.yaml --output results_new/batch-run03 --seed 3816
