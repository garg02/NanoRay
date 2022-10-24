# image setup

### Before starting, note that all nodes must have the same version of Python and Ray running

## guppy-mm2-metrics-image - (60GB RAM, 1 NVME, 200GB disk)
### install ray and others
sudo apt-get update
sudo apt-get -y install time rsync git-all make python3-pip python3-venv wget autoconf automake gcc perl libdeflate-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev
sudo pip install "ray==2.0.0rc1" "ray[default]" requests python-crontab

### install nvidia gpu driver 
curl https://raw.githubusercontent.com/GoogleCloudPlatform/compute-gpu-installation/main/linux/install_gpu_driver.py --output install_gpu_driver.py
sudo python3 install_gpu_driver.py

### install gcp ops agent 
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install

### install gcp ops agent for GPU
sudo mkdir -p /opt/google 
cd /opt/google
sudo git clone https://github.com/GoogleCloudPlatform/compute-gpu-monitoring.git 
cd /opt/google/compute-gpu-monitoring/linux
sudo python3 -m venv venv
sudo venv/bin/pip install wheel
sudo venv/bin/pip install -Ur requirements.txt
sudo cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
sudo systemctl daemon-reload
sudo systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
cd ~

### install guppy-basecaller
sudo mkdir -p /opt/ont/
sudo wget -c https://cdn.oxfordnanoportal.com/software/analysis/ont-guppy_6.2.1_linux64.tar.gz && sudo tar -xzf ont-guppy_6.2.1_linux64.tar.gz -C /opt/ont/

### install minimap2
sudo mkdir -p /opt/mm/
wget -c https://github.com/lh3/minimap2/releases/download/v2.24/minimap2-2.24_x64-linux.tar.bz2 && sudo tar -jxvf minimap2-2.24_x64-linux.tar.bz2 -C /opt/mm

### install samtools
wget -c https://github.com/samtools/samtools/releases/download/1.16/samtools-1.16.tar.bz2 && sudo tar -jxvf samtools-1.16.tar.bz2
cd samtools-1.16
sudo ./configure --prefix=/usr/ --with-libdeflate
sudo make
sudo make install
cd ~

### install go 
wget -c https://go.dev/dl/go1.19.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.19.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

### install metrics 
git clone https://github.com/garg02/custom-metric-reporter-gcp.git
cd custom-metric-reporter-gcp
go mod init metrics
go mod tidy
go build metrics.go
chmod +x metrics
sudo cp metrics /usr/local/bin/
cd ~

## bam-merge-image - (52GB RAM, 1 NVME, 50GB disk)
### install ray and others
sudo apt-get update
sudo apt-get -y install time git-all rsync make python3-pip python3-venv wget autoconf automake gcc perl libdeflate-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev python-is-python3
sudo pip install requests python-crontab "ray==2.0.0rc1"
sudo pip install "ray[default]"

### install gcp ops agent
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install

### install samtools
wget -c https://github.com/samtools/samtools/releases/download/1.16/samtools-1.16.tar.bz2 && sudo tar -jxvf samtools-1.16.tar.bz2
cd samtools-1.16
sudo ./configure --prefix=/usr/ --with-libdeflate
sudo make
sudo make install
cd ~


## pmdv-image (60GB RAM, 1 NVME, 200GB disk)
sudo apt-get update
sudo apt-get -y install time rsync make python3-pip python3-venv wget autoconf automake gcc perl libdeflate-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev
sudo pip install requests python-crontab "ray==2.0.0rc1"
sudo pip install "ray[default]"

### install nvidia gpu driver 
curl https://raw.githubusercontent.com/GoogleCloudPlatform/compute-gpu-installation/main/linux/install_gpu_driver.py --output install_gpu_driver.py
sudo python3 install_gpu_driver.py

### install gcp ops agent 
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install

### install gcp ops agent for GPU
sudo mkdir -p /opt/google 
cd /opt/google
sudo git clone https://github.com/GoogleCloudPlatform/compute-gpu-monitoring.git 
cd /opt/google/compute-gpu-monitoring/linux
sudo python3 -m venv venv
sudo venv/bin/pip install wheel
sudo venv/bin/pip install -Ur requirements.txt
sudo cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
sudo systemctl daemon-reload
sudo systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
cd ~

### install samtools
wget -c https://github.com/samtools/samtools/releases/download/1.16/samtools-1.16.tar.bz2 && sudo tar -jxvf samtools-1.16.tar.bz2
cd samtools-1.16
sudo ./configure --prefix=/usr/ --with-libdeflate
sudo make
sudo make install
cd ~

### install nvidia-docker
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
      && curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg \
      && curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
            sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
            sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list
sudo apt-get update
sudo apt-get install -y nvidia-docker2
sudo systemctl restart docker
sudo docker run --rm --gpus all nvidia/cuda:11.0.3-base-ubuntu20.04 nvidia-smi

### install PMDV
sudo docker pull kishwars/pepper_deepvariant:r0.8-gpu

## sniffles2-image - (60GB RAM, 1 NVME, 16GB disk)
### install ray and others
sudo apt-get update
sudo apt-get -y install time git-all rsync python3-pip python-is-python3
sudo pip install requests python-crontab "ray==2.0.0rc1"
sudo pip install "ray[default]"
sudo pip install sniffles

### install gcp ops agent
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install


## ray-head-image
### install ray and others
sudo apt-get update
sudo apt-get -y install time git-all rsync python3-pip python-is-python3 wget
sudo pip install "ray==2.0.0rc1" requests google-api-python-client==1.7.8 cryptography paramiko
sudo pip install -U "ray[default]"
