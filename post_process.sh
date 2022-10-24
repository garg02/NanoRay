ray exec NanoRay.yaml "curl http://metadata.google.internal/computeMetadata/v1/instance/hostname -H Metadata-Flavor:Google | cut -d . -f1"
FILE=/tmp/ray-timeline-2022-09-22_06-15-12.json
HEAD=ray-nanoray-head-6a618a96-compute
gcloud compute scp $HEAD:/tmp/timeline.json ~/Desktop/

# create image for instance with NVME
gcloud compute machine-images create bam-merge-image --source-instance=bam-merge-image --storage-location=us-central1 --source-instance-zone=us-central1-c
gcloud compute machine-images create pmdv-image --source-instance=pmdv-image --storage-location=us-central1 --source-instance-zone=us-central1-f
gcloud compute machine-images create guppy-mm2-metrics-image --source-instance=guppy-mm2-metrics-image-1 --storage-location=us-central1 --source-instance-zone=us-central1-f
gcloud compute machine-images create sniffles2-image --source-instance=sniffles-image --storage-location=us-central1 --source-instance-zone=us-central1-f