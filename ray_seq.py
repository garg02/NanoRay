from queue import Empty
from typing_extensions import OrderedDict
import ray
import os
import subprocess
import time
import glob
from datetime import datetime
import requests
from crontab import CronTab
import re
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.util.queue import Queue, Empty
import csv

# Config variables
FAST5_BUCKET = 'gs://ultra_rapid_prom_data/prom/HG002_No_BC/no_sample/20210525_6H/*.fast5'
BUCKET = 'gs://fastq_s22/no_sample/220925' # For NanoRay to use; exclude '/' at the end of URI
PATIENT_SEX = "M"
DONE_BOOL = True # Has sequencing finished; if False, URI will signal completion
DONE_LOCATION = 'gs://fastq_s22/done.txt' # not used when DONE_BOOL is True
BARCODE_DONE = False
BARCODE_NUM = 0 # not used when BARCODE_DONE is False
MAX_READS_PER_FAST5 = 10000
MAX_FAST5s_IN_BATCH = 2
NUM_GUPPY_GPUs = 1
NUM_MM_CPUs = 10
MM2_SAMTOOLS_CPUs = 6
TOTAL_MERGE_CPUs = 8 # num BAM merge instances * num cpus in each BAM merge instance
NUM_BATCHES_IN_MERGE = 2 # number of batches in chr specific intermediate bams
NUM_PMDV_INSTANCES = 1
PMDV_CPUs = 12
PMDV_GPUs = 1
FINAL_BAM_SAMTOOLS_CPUs = 4
SNIFFLES_CPUs = 16
REFERENCE = "gs://ur_wgs_test_data/GRCh37.mmi"
CHR_REF_FASTA = 'gs://ur_wgs_test_data/GRCh37_chr_fasta/GRCh37_chr*.fa' # '*' is a placeholder for chr
SNIFFLES2_ANNOT = {"filename": "human_hs37d5.trf.bed", "link": "https://github.com/fritzsedlazeck/Sniffles/raw/master/annotations/human_hs37d5.trf.bed"}
CHR_BAM_MERGE_CPUs = TOTAL_MERGE_CPUs/26 if TOTAL_MERGE_CPUs < 26 else TOTAL_MERGE_CPUs//26
# Priority Queue order of chromosomes for PMDV
PMDV_MALE = list(range(1,14)) + [16, 21, 17, 14, 20, 15, "X", 19, 18, "Y", 22, "MT"]
PMDV_FEMALE = list(range(1,13)) + ["X", 16, 17, 21, 15, 14, 18, 13, 20, 19, 22, "Y", "MT"]
SNIFFLES_QUEUE = [16, 1, 2, 4, 3, 5, 6, 7, 8, 12, 9, 10, 11, 13, 17, "X", 14, 15, 18, 20, 19, 21, 22, "Y", "MT"]
SNIFFLES_RATIO = [16]*7 + [8]*8 + [4]*3 + [2]*5 + [1]*2

ray.init(address='auto')
def reset_file_descriptor(log_file):
	# refreshes file descriptor so subprocess will write at the end of the file
	log_filepath = os.path.realpath(log_file.name)
	log_file.close()
	return open(log_filepath, "a")

def process_command(code, log_file, task_desc, log_dest):
	# process exit code
	if code > 0:
		print(f'error occured during {task_desc}: look in uploaded log at {log_dest}')
		log_file.write(f'{task_desc} failed with non-zero code {str(code)}, exiting task \n')
		close_log(log_file, log_dest)
		os._exit(code)
	log_file.write(f'{task_desc} completed succesfully \n')

def run_subprocess(command, log_file, log_dest, task_desc, shell_var=False):
	# wrapper for run.subprocess that also manages logging and error exit codes
	log_file = reset_file_descriptor(log_file)
	p = subprocess.run(command, shell=shell_var, stdout=log_file, stderr=subprocess.STDOUT)
	log_file = reset_file_descriptor(log_file)
	process_command(p.returncode, log_file, task_desc, log_dest)
	return log_file

def run_actor_subprocess(command, log_file, task_desc, shell_var=False):
	# wrapper for actor run.subprocess; ensures actor doesnt crash when subprocess fails
	success = False
	for _ in range(5):
		log_file = reset_file_descriptor(log_file)
		p = subprocess.run(command, shell=shell_var, stdout=log_file, stderr=subprocess.STDOUT)
		log_file = reset_file_descriptor(log_file)
		if p.returncode == 0:
			log_file.write(f'{task_desc} completed succesfully \n')
			success = True
			break
		print(f'error occured during {task_desc}: look in uploaded log for {log_file.name}')
		log_file.write(f'{task_desc} failed with code {str(p.returncode)}, retrying \n')
	return log_file, success

def open_guppymm2_log(batch_name, task_name):
	# opens log and writes header for guppy_mm batch logs
	log_filename = f'/data/{batch_name}/{batch_name}.log'
	log_file = open(log_filename, "a")
	log_file.write(f'\n****************** {(datetime.now())} {task_name} started for batch {batch_name}\n')
	return log_file

def open_chr_log(chrom, task_name):
		# opens log and writes header for chrom specific logs
		log_name = f'/data/{chrom}/{chrom}.log'
		log_file = open(log_name, "a")
		log_file.write(f'\n****************** {str(datetime.now())} {task_name} task started for chrom {chrom}\n')		
		return log_file

def close_log(log_file, log_dest):
	# closes log and uploads file to appropriate GCS bucket
	log_filepath = os.path.realpath(log_file.name)
	log_file.write(f'***************** {(datetime.now())} task finished\n')
	log_file.close()
	upload_command = ["gcloud", "alpha", "storage", "cp", log_filepath, log_dest]
	subprocess.run(upload_command)
	return

def initialize_actor_log(chrom, log_dest):
	log_file = open_chr_log(chrom, "get_node")
	node_id = ray.get_runtime_context().node_id
	log_file.write(f'Chr {chrom} has been assigned to node {node_id}\n')
	close_log(log_file, log_dest)
	return node_id
	
def create_metrics_file(metric):
	meta_header = {'Metadata-Flavor': 'Google'}
	exec = "/usr/local/bin/metrics"
	project_url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
	instance_url = "http://metadata.google.internal/computeMetadata/v1/instance/id"
	zone_url = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
	project_id = requests.get(project_url, headers=meta_header).text
	instance_id = requests.get(instance_url, headers=meta_header).text
	zone_id = requests.get(zone_url, headers=meta_header).text.split('/')[-1]
	metric_file = f'/tmp/{metric}.txt'
	with open(metric_file, "a") as f:
		f.write(f'PROJECT_ID={project_id}\n')
		f.write(f'INSTANCE_ID={instance_id}\n')
		f.write(f'ZONE_ID={zone_id}\n')
		f.write(f'{metric}=none\n')
	cron = CronTab(user = os.getlogin())
	arg = f'{exec} -f {metric_file} >> {metric}.out 2>> {metric}.err'
	job = cron.new(command=arg)
	job.minute.every(1)
	cron.write()

def write_batch_metric(metric, batch_name): # potential concurrency issue
	metric_file = f'/tmp/{metric}.txt'
	with open(metric_file, 'r') as f :
		file = f.read()
	search = f'^{metric}=.*'
	replace = f'{metric}={str(batch_name)}'
	file = re.sub(search, replace, file, flags=re.M)
	with open(metric_file, 'w') as f:
		f.write(file)

def get_unmerged_bams(bucket, chrom):
	split_folder = f'{bucket}/bams/{chrom}/batch_split/*.bam'
	merged_folder = f'{bucket}/bams/{chrom}/intermediate/*.bam'
	split_set, merged_set = set(), set()
	for folder, col in [(split_folder, split_set), (merged_folder, merged_set)]:
		ls_command = ["gcloud", "alpha", "storage", 'ls', folder]
		bams = subprocess.run(ls_command, stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()
		bams = [os.path.splitext(os.path.basename(x))[0] for x in bams]
		if folder == merged_folder:
			for bam in bams:
				col.update(bam.split("-"))
		else:
			col.update(bams)
	return split_set - merged_set

@ray.remote(max_retries=2, num_gpus=NUM_GUPPY_GPUs, resources={'guppy-mm2': 0.01})
def run_guppy(batch_name, max_reads_per_fast5, barcode_done, bucket, fast5s, reference):
	# huge function needed to ensure all subparts run sequentially
	# change when parallel placement groups issue is fixed

	# create batch dir, log file, and metrics files; 
	# download GRCh37.mmi; assigns node for batch
	batch_dir = f'/data/{batch_name}/'
	os.makedirs(batch_dir, exist_ok=True)
	log_file = open_guppymm2_log(batch_name, "get_node")
	log_dest = f'{bucket}/batches/{batch_name}/{batch_name}.log'
	
	batch_labels = ["mm2_batch_num", "guppy_batch_num"]
	if not os.path.isfile(f'/tmp/{batch_labels[0]}.txt'):
		for batch_label in batch_labels: create_metrics_file(batch_label)

	if not os.path.isfile(f'/data/{os.path.basename(reference)}'):
		# download mmi file using gcloud storage CLI
		download_command = ["gcloud", "alpha", "storage", "cp", reference, "/data/"]
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"Reference download", shell_var=False)

	node_id = ray.get_runtime_context().node_id
	log_file.write(f'Batch {batch_name} has been assigned to node {node_id}\n')
	close_log(log_file, log_dest) # update gcp log
	
	# downloads fast5 files for a batch
	log_file = open_guppymm2_log(batch_name, "download_guppy")
	
	# create fast5 directory 
	fast5_dir = f'/data/{batch_name}/fast5/'
	os.makedirs(fast5_dir, exist_ok=True)
	log_file.write(f'Batch {batch_name} fast5 folder {fast5_dir} exists \n')
	
	# download files using gcloud storage CLI
	download_command = ["gcloud", "alpha", "storage", "cp", fast5_dir]
	download_command[4:4] = fast5s #add the fast5 filepaths to command
	log_file.write("Starting fast5 download \n")
	log_file = run_subprocess(download_command, log_file, log_dest, \
		"Download", shell_var=False)
	close_log(log_file, log_dest)

	# ensures GPU is running, runs Guppy, and calculates pass ratio
	log_file = open_guppymm2_log(batch_name, "run_guppy")
	write_batch_metric("guppy_batch_num", batch_name)

	# test NVIDIA driver
	for _ in range(5):
		log_file.write("Checking if CUDA driver is working \n")
		log_file = reset_file_descriptor(log_file)
		p = subprocess.run(["nvidia-smi"], stdout=log_file, stderr=subprocess.STDOUT)
		time.sleep(5)
		if p.returncode == 0: break
	log_file = reset_file_descriptor(log_file)
	process_command(p.returncode, log_file, "NVIDIA driver", log_dest)
	
	fast5_dir = f'/data/{batch_name}/fast5/'
	num_fast5s = len(glob.glob1(fast5_dir, '*.fast5'))
	max_reads = num_fast5s * max_reads_per_fast5
	config_file = "/opt/ont/ont-guppy/data/dna_r9.4.1_450bps_hac_prom.cfg"
	
	# create fastq directory 
	fastq_dir = f'/data/{batch_name}/fastq/'
	os.makedirs(fastq_dir, exist_ok=True)
	log_file.write(f'Created batch fastq folder: {fastq_dir} \n')
	
	# run Guppy using CLI
	log_file.write(f'Starting basecalling job for {str(num_fast5s)} fast5 file(s) \n')
	guppy_command = ["/opt/ont/ont-guppy/bin/guppy_basecaller", "--config", config_file, 
	"-i", fast5_dir, "-s", fastq_dir, "-x", "cuda:all", 
	"-q", str(max_reads), "--read_batch_size", str(max_reads)]
	if barcode_done:
		guppy_command += ["--barcode_kits", "EXP-NBD104"]
	
	log_file = run_subprocess(guppy_command, log_file, log_dest, \
		"Guppy basecalling", shell_var=False)
	
	# calculate ratio of passed to total
	log_file.write("Sequencing summary \n")
	passed, total = 0, 0
	summary_path = f'{fastq_dir}/sequencing_summary.txt'
	with open(summary_path) as file:
		for line in file:
			l = line.split('\t')
			if l[9] == "passes_filtering": # exclude header line
				continue
			if l[9] == "TRUE":
				passed += int(l[13])
			total += int(l[13])
	log_file.write(f'Passed/Total = {passed/total:.2f} \n')

	# upload summary file
	dest_path = f'{bucket}/batches/{batch_name}/sequencing_summary.txt'
	upload_command = ["gcloud", "alpha", "storage", "cp", summary_path, dest_path]
	log_file = run_subprocess(upload_command, log_file, log_dest, \
		"sequencing summary upload", shell_var=False)
	close_log(log_file, log_dest)
	return node_id

@ray.remote(max_retries=2, num_cpus=NUM_MM_CPUs, resources={'guppy-mm2': 0.01}) 
def run_minimap2(batch_name, barcode_done, barcode_num, bucket):
	# determines if Minimap should run and runs minimap2
	log_file = open_guppymm2_log(batch_name, "run_minimap2")
	log_dest = f'{bucket}/batches/{batch_name}/{batch_name}.log'

	# As DAG for batches is prescheduled, every function in the pipeline 
	# must be run, even if the batch is empty.
	# For all subsequent steps, if batch_name is False, it returns immediately
	pass_fastq_dir = f'/data/{batch_name}/fastq/pass/'
	if barcode_done:
		pass_fastq_dir = f'{pass_fastq_dir}/{str(barcode_num)}'

	# count number of passed fastqs
	fastqs = glob.glob1(pass_fastq_dir, "*.fastq")
	if len(fastqs) == 0:
		log_file.write("No fastqs in pass folder; no alignment required \n")
		close_log(log_file, log_dest)
		return False
	else:
		log_file.write("Minimap2 continuing \n")
		write_batch_metric("mm2_batch_num", batch_name)

	# get arguments for minimap2
	fastqs = [f'{pass_fastq_dir}/{fastq}' for fastq in fastqs]
	ref = f'/data/GRCh37.mmi'
	bam = f'/data/{batch_name}/{batch_name}.bam'

	# run minimap2 using CLI, must use shell=True with samtools
	log_file.write(f'Starting alignment job for {str(len(fastqs))} fastq file(s) \n')
	minimap2_command = ["/opt/mm/minimap2-2.24_x64-linux/minimap2", "-ax", 
	"map-ont", "--MD", "-t", str(NUM_MM_CPUs), ref, "|", 
	"samtools", "view", "-F", "0x904", "-h", "-@", str(MM2_SAMTOOLS_CPUs), "|", 
	"samtools", "sort", "-@", str(MM2_SAMTOOLS_CPUs), "|", 
	"samtools", "view", "-hb","-@", str(MM2_SAMTOOLS_CPUs), ">", bam]
	minimap2_command[7:7] = fastqs
	minimap2_command = ' '.join(minimap2_command)
	log_file = run_subprocess(minimap2_command, log_file, log_dest, \
		"Minimap2", shell_var=True)
	close_log(log_file, log_dest)
	return batch_name

@ray.remote(max_retries=2, num_cpus=MM2_SAMTOOLS_CPUs, resources={'guppy-mm2': 0.01})
def create_index(batch_name, bucket):
	# checks if batch is valid and indexes it 
	if not batch_name:
		return False
	log_file = open_guppymm2_log(batch_name, "create_index")
	log_dest = f'{bucket}/batches/{batch_name}/{batch_name}.log'

	# run samtools index using CLI
	bam = f'/data/{batch_name}/{batch_name}.bam'
	index_command = ["samtools", "index", "-@"+str(MM2_SAMTOOLS_CPUs), bam]
	index_command = ' '.join(index_command)
	log_file = run_subprocess(index_command, log_file, log_dest, \
		"samtools index", shell_var=True)
	close_log(log_file, log_dest)
	return batch_name

@ray.remote(max_retries=2, num_cpus=MM2_SAMTOOLS_CPUs, resources={'guppy-mm2': 0.01}) 
def split_bam(batch_name, chrom, bucket):
	# checks if batch is valid and splits it for the given chromosome
	if not batch_name:
		return False
	log_name = f'/data/{batch_name}/{batch_name}_{chrom}.log'
	log_file = open(log_name, "a")
	log_file.write(f'\n {str(datetime.now())} split_bam task (re)started for {batch_name}_{chrom} \n')
	log_dest = f'{bucket}/batches/{batch_name}/{batch_name}_{chrom}.log'

	# create chromosome directory 
	chr_dir = f'/data/{chrom}/batch_split/'
	os.makedirs(chr_dir, exist_ok=True)
	log_file.write(f'chr {chrom} directory {chr_dir} exists \n')

	# run samtools view/quickcheck using CLI
	bam = f'/data/{batch_name}/{batch_name}.bam'
	bam_chr = f'{chr_dir}/{batch_name}.bam'
	split_comm = ["samtools", "view", "-b", "-@", str(MM2_SAMTOOLS_CPUs), 
	bam, chrom, ">", bam_chr]
	split_comm = ' '.join(split_comm)
	log_file = run_subprocess(split_comm, log_file, log_dest, \
		"samtools view", shell_var=True)
	
	check_command = ["samtools", "quickcheck", bam_chr]
	log_file = run_subprocess(check_command, log_file, log_dest, \
		"samtools check", shell_var=False)
	close_log(log_file, log_dest)
	return batch_name

@ray.remote(max_retries=2, resources={'guppy-mm2': 0.01}) 
def upload_split(batch_name, chrom, bucket):
	# checks if batch is valid and uploads it for the given chromosome
	if not batch_name:
		return False
	log_name = f'/data/{batch_name}/{batch_name}_{chrom}.log'
	log_file = open(log_name, "a")
	log_file.write(f'\n {str(datetime.now())} upload_split task (re)started for {batch_name}_{chrom} \n')
	log_dest = f'{bucket}/batches/{batch_name}/{batch_name}_{chrom}.log'

	# upload split bams using CLI
	bam_chr = f'/data/{chrom}/batch_split/{batch_name}.bam'
	dest_folder = f'{bucket}/bams/{chrom}/batch_split/{batch_name}.bam'
	upload_command = ["gcloud", "alpha", "storage", "cp", bam_chr, dest_folder]
	log_file = run_subprocess(upload_command, log_file, log_dest, \
		"chr-wise bam upload", shell_var=False)
	close_log(log_file, log_dest)
	return batch_name

@ray.remote(max_retries=0, num_returns=25, num_cpus=0.1, resources={'head': 0.01})
def manage_guppy_mm2(batch_name, fast5_list, chromosomes):
	# ensure guppy_mm2 runs on same node
	error = True
	for i in range(2):
		try:
			curr_node_id = ray.get(run_guppy.options(name=f'guppy_{batch_name}').remote(
				batch_name, MAX_READS_PER_FAST5, BARCODE_DONE, BUCKET, fast5_list, REFERENCE))

			node_strategy = NodeAffinitySchedulingStrategy(node_id=curr_node_id, soft=False)
			minimap_future = run_minimap2.options(name=f'minimap2_{batch_name}', scheduling_strategy=node_strategy
			).remote(batch_name, BARCODE_DONE, BARCODE_NUM, BUCKET)

			index_future = create_index.options(name=f'index_{batch_name}',
				scheduling_strategy=node_strategy).remote(minimap_future, BUCKET)
			
			upload_futures = []
			for chr in chromosomes:
				chr_name = f'{batch_name}_{chr}'
				split_future = split_bam.options(name=f'split_{chr_name}', 
				scheduling_strategy=node_strategy).remote(index_future, chr, BUCKET)

				upload_future = upload_split.options(name=f'upload_{chr_name}', 
				scheduling_strategy=node_strategy).remote(split_future, chr, BUCKET)

				upload_futures.append(upload_future)
			upload_futures = ray.get(upload_futures)
			error = False
			break
		except Exception as e:
			print(e)
			print(f'Batch {batch_name} failed a task 5 times. Retrying again max {2-i} times')
	if error:
		print(f'batch {batch_name} with fast5s {fast5_list} will be skipped')
	upload_futures = [False]*25 if error else upload_futures
	return tuple(upload_futures)

@ray.remote(num_cpus=CHR_BAM_MERGE_CPUs, max_restarts=10, max_task_retries=-1, resources={'chrbam': 0.01})
class ChrBamMerger:
	def __init__(self, bucket, chrom):
		# sets up variables, chr log and download/merged directory for bam merge
		self.bucket = bucket
		self.chrom = str(chrom)
		self.bams_processing = set()
		self.merged_dir = f'/data/{self.chrom}/intermediate/'
		os.makedirs(self.merged_dir, exist_ok=True)
		self.download_dir = f'/data/{self.chrom}/batch_split/'
		os.makedirs(self.download_dir, exist_ok=True)
		self.log_dest = f'{self.bucket}/bams/{self.chrom}/{self.chrom}.log'

		initialize_actor_log(self.chrom, self.log_dest)

	def get_bams(self, *batch_names):
		# filters bam names, creates bam directory, and downloads them
		log_file = open_chr_log(self.chrom, "get_bams")
		bam_batch = get_unmerged_bams(self.bucket, self.chrom) - self.bams_processing
		self.bams_processing.update(bam_batch)

		if len(bam_batch) <= 1:
			log_file.write(f'No need to merge {len(bam_batch)} bams\n')
			close_log(log_file, self.log_dest)
			return False
		
		# download files using gcloud storage CLI
		download_command = ["gcloud", "alpha", "storage", "cp", self.download_dir]
		download_command[4:4] = [f'{self.bucket}/bams/{self.chrom}/batch_split/{bam}.bam' \
			 for bam in bam_batch]
		log_file.write("Starting split bam download \n")
		log_file, success = run_actor_subprocess(download_command, log_file, \
			"Bam download", shell_var=False)
		close_log(log_file, self.log_dest)
		if not success:
			return False
		return bam_batch

	def merge_bams(self, bam_batch):
		# checks if merge-batch is valid and merges to intermediate bam
		if not bam_batch:
			return False, bam_batch
		
		log_file = open_chr_log(self.chrom, "merge_bams")
		bam_filepaths = [f'{self.download_dir}/{bam}.bam' for bam in bam_batch]
		out_file = f'{"-".join(bam_batch)}.bam'
		out_path = f'{self.merged_dir}/{out_file}'
		merge_command = ["samtools", "merge", "-@", \
			str(MM2_SAMTOOLS_CPUs), out_path] + bam_filepaths
		merge_command = ' '.join(merge_command)
		log_file, success = run_actor_subprocess(merge_command, log_file, \
			 "Split bam merge", shell_var=True)
		if not success:
			close_log(log_file, self.log_dest)
			return False
		index_command = ["samtools", "index", "-@", \
			str(MM2_SAMTOOLS_CPUs), out_path]
		index_command = ' '.join(index_command)
		log_file, success = run_actor_subprocess(index_command, log_file, \
			 "merged bam index", shell_var=True)
		close_log(log_file, self.log_dest)
		if not success:
			return False
		return (out_path, bam_batch)

	def upload_merged(self, input):
		# upload merged intermediate bam to GCS
		bam_path, bam_batch = input
		if not bam_path:
			return False

		log_file = open_chr_log(self.chrom, f'upload_merged {bam_path}')
		bam_file = os.path.basename(bam_path)
		dest_path = f'{self.bucket}/bams/{self.chrom}/intermediate/{bam_file}'
		upload_command = ["gcloud", "alpha", "storage", "cp", bam_path, dest_path]
		log_file, success = run_actor_subprocess(upload_command, log_file, \
			"merged bam upload", shell_var=False)
		self.bams_processing -= bam_batch
		close_log(log_file, self.log_dest)
		if not success:
			return False
		return bam_file

@ray.remote(max_retries=2, num_gpus=0.01, resources={'pmdv': 0.01})
def run_final_merge(bucket, chr):
	# merges all intermediate bam files into one final bam file
	chr_dir = f'/data/{chr}/'
	download_dir = f'{chr_dir}/download/'
	os.makedirs(download_dir, exist_ok=True)
	log_dest = f'{bucket}/bams/{chr}/{chr}_merge.log'
	node_id = ray.get_runtime_context().node_id
	log_file = open_chr_log(chr, "final_bam_merge")

	# download files using gcloud storage CLI
	unmerged_bams = get_unmerged_bams(bucket, chr)
	download_bams = [f'{bucket}/bams/{chr}/batch_split/{bam}.bam' for bam in unmerged_bams]
	merged_folder = f'{bucket}/bams/{chr}/intermediate/*.bam'
	ls_commans = ["gcloud", "alpha", "storage", 'ls', merged_folder]
	merged_bams = subprocess.run(ls_commans, stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()
	download_bams += merged_bams
	download_command = ["gcloud", "alpha", "storage", "cp", download_dir]
	download_command[4:4] = download_bams
	log_file = run_subprocess(download_command, log_file, log_dest, \
		"intermediate bam download", shell_var=False)
	close_log(log_file, log_dest)

	open_chr_log(chr, "final bam merge")
	merged_file = f'/data/{chr}/merged.bam'
	download_bams = glob.glob(f'{download_dir}/*.bam')

	merge_command = ["samtools", "merge", "-f", "-@", str(FINAL_BAM_SAMTOOLS_CPUs), \
		merged_file] + download_bams
	merge_command = ' '.join(merge_command)
	log_file = run_subprocess(merge_command, log_file, log_dest, \
		"Final bam merge", shell_var=True)
	close_log(log_file, log_dest)
	return node_id

@ray.remote(max_retries=2, num_cpus=FINAL_BAM_SAMTOOLS_CPUs, resources={'pmdv': 0.01})
def index_final_bam(bucket, chr):
	open_chr_log(chr, "index final merged bam")
	log_dest = f'{bucket}/bams/{chr}/{chr}_merge.log'
	log_file = open_chr_log(chr, "final_bam_index")

	# run samtools index using CLI
	merged_file = f'/data/{chr}/merged.bam'
	index_command = ["samtools", "index", "-@", str(FINAL_BAM_SAMTOOLS_CPUs), merged_file]
	index_command = ' '.join(index_command)
	log_file = run_subprocess(index_command, log_file, log_dest, \
		"samtools index", shell_var=True)
	close_log(log_file, log_dest)
	return merged_file

@ray.remote(max_retries=2, resources={'pmdv': 0.01})
def upload_final_bam(bucket, chr, merged_file):
	open_chr_log(chr, "uploading final merged bam and index")
	log_dest = f'{bucket}/bams/{chr}/{chr}_merge.log'
	log_file = open_chr_log(chr, "final_bam_merge")
	
	# upload split bams using CLI
	upload_files = glob.glob(f'{merged_file}*')
	dest_folder = f'{bucket}/bams/{chr}/'
	upload_command = ["gcloud", "alpha", "storage", "cp", dest_folder]
	upload_command[4:4] = upload_files
	log_file = run_subprocess(upload_command, log_file, log_dest, \
		"final merged bam/bai upload", shell_var=False)
	close_log(log_file, log_dest)
	return chr

@ray.remote(max_retries=0, num_cpus=0.1, resources={'head': 0.01})
def manage_final_bam_merge(chr, first):
	# ensure the merge runs on same node
	error = True
	task_resources = {"pmdv": 0.01}
	needed_cpus = FINAL_BAM_SAMTOOLS_CPUs
	for i in range(2):
		try:
			if first: 
				task_resources = {"pmdv": 0.01, "first": 1}
				needed_cpus *= 2

			curr_node_id = ray.get(run_final_merge.options(
				name=f'final_bam_merge_{chr}',
				num_cpus=needed_cpus,
				num_gpus=0.01,
				resources=task_resources).remote(BUCKET, chr))

			node_strategy = NodeAffinitySchedulingStrategy(node_id=curr_node_id, soft=False)
			index_future = index_final_bam.options(name=f'final_bam_index_{chr}',
				scheduling_strategy=node_strategy).remote(BUCKET, chr)
			
			upload_future = upload_final_bam.options(name=f'final_bam_upload_{chr}',
				scheduling_strategy=node_strategy).remote(BUCKET, chr, index_future)
			
			ray.get(upload_future)
			error = False
			break
		except Exception as e:
			print(e)
			print(f'Chromosome {chr} failed a task 5 times. Retrying again max {2-i} times')
	if error:
		print(f'Chromosome {chr} will be skipped')
		return False
	return curr_node_id

@ray.remote(max_retries=2, num_cpus=PMDV_CPUs, num_gpus=PMDV_GPUs, resources={'pmdv': 0.01})
def run_pmdv(bucket, chr):
	# runs pmdv on merged file
	input_dir = f'/data/{chr}/'
	os.makedirs(input_dir, exist_ok=True)
	output_dir = f'{input_dir}/pmdv/'
	log_dest = f'{bucket}/pmdv/{chr}/{chr}.log'
	node_id = ray.get_runtime_context().node_id
	log_file = open_chr_log(chr, "run_pmdv")

	if not os.path.isfile(f'{input_dir}/merged.bam'):
	# download merged bam file using gcloud storage CLI
		merged_dir = f'{bucket}/bams/{chr}'
		merged_bam_files = [f'{merged_dir}/merged.bam', f'{merged_dir}/merged.bam.bai']
		download_command = ["gcloud", "alpha", "storage", "cp", input_dir]
		download_command[4:4] = merged_bam_files
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"final bam download", shell_var=False)

	chr_ref_fasta = CHR_REF_FASTA.replace("*", str(chr))
	ref_filename = os.path.basename(chr_ref_fasta)
	if not os.path.isfile(f'{input_dir}/{ref_filename}'):
		# download mmi file using gcloud storage CLI
		download_command = ["gcloud", "alpha", "storage", "cp", chr_ref_fasta, input_dir]
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"fasta reference download", shell_var=False)
		close_log(log_file, log_dest)

	open_chr_log(chr, "pmdv")
	merged_file = f'/data/{chr}/merged.bam'
	ref_file = f'{input_dir}/{ref_filename}'

	# test effect of "--dv_add_hp_channel" and "--phased_output"
	pmdv_command = ["sudo", "docker", "run", "--ipc=host", "--gpus", "all", \
	"-v", f'{input_dir}:{input_dir}', "-v", f'{output_dir}:{output_dir}', 
	"kishwars/pepper_deepvariant:r0.8-gpu", "run_pepper_margin_deepvariant", 
	"call_variant", "-b", merged_file, "-f", ref_file, "-o", output_dir, 
	"-t", str(PMDV_CPUs), "-g", "--ont_r9_guppy5_sup", "--phased_output",
	"--keep_intermediate_bam_files", "--dv_alt_aligned_pileup", "rows", 
	"--dv_realign_reads", "false", "--dv_min_mapping_quality", "1", 
	"--dv_min_base_quality", "1", "--dv_sort_by_haplotypes", "true",
	"--dv_parse_sam_aux_fields", "true", "--dv_use_multiallelic_mode", "true"]

	log_file = run_subprocess(pmdv_command, log_file, log_dest, \
		"PMDV run", shell_var=False)
	close_log(log_file, log_dest)

	return node_id
	
@ray.remote(max_retries=2, resources={'pmdv': 0.01})
def upload_pmdv(bucket, chr):
	# upload pmdv outputs
	input_dir = f'/data/{chr}/pmdv/'
	dest_folder = f'{bucket}/pmdv/{chr}'
	log_dest = f'{dest_folder}/{chr}.log'
	log_file = open_chr_log(chr, "upload_pmdv")
	rename_command = ["sudo", "mv", \
		f'{input_dir}/PEPPER_MARGIN_DEEPVARIANT_FINAL_OUTPUT.vcf.gz',
		f'{input_dir}/chr{chr}_PMDV_FINAL_OUTPUT.vcf.gz']
	log_file = run_subprocess(rename_command, log_file, log_dest, \
		"pmdv rename", shell_var=False)

	# upload split bams using CLI
	upload_files = [f'chr{chr}_PMDV_FINAL_OUTPUT.vcf.gz', \
		"/intermediate_files/PHASED.PEPPER_MARGIN.haplotagged.bam",
		"/intermediate_files/PHASED.PEPPER_MARGIN.haplotagged.bam.bai"]
	upload_files = [f'{input_dir}/{file}' for file in upload_files]
	upload_command = ["gcloud", "alpha", "storage", "cp", dest_folder]
	upload_command[4:4] = upload_files
	log_file = run_subprocess(upload_command, log_file, log_dest, \
		"pmdv upload", shell_var=False)
	close_log(log_file, log_dest)
	return chr

@ray.remote(max_retries=0, num_cpus=.1, resources={'head': 0.01})
def manage_pmdv(chr, bam_node_id):
	if bam_node_id is False:
		print("bam merge for chr {chr} failed. Skipping pmdv")
		return False
	error = True
	for i in range(2):
		try:
			initial_node_strategy = NodeAffinitySchedulingStrategy(
				node_id=bam_node_id, soft=True)
			curr_node_id = ray.get(run_pmdv.options(
				scheduling_strategy=initial_node_strategy, name=f'run_pmdv_{chr}'
				).remote(BUCKET, chr))

			node_strategy = NodeAffinitySchedulingStrategy(node_id=curr_node_id, soft=False)
			upload_future = upload_pmdv.options(name=f'upload_pmdv_{chr}',
				scheduling_strategy=node_strategy).remote(BUCKET, chr)
			
			ray.get(upload_future)
			error = False
			break
		except Exception as e:
			print(e)
			print(f'PMDV for Chromosome {chr} failed a task 5 times. Retrying again max {1-i} time')
	if error:
		print(f'PMDV for chromosome {chr} will be skipped')
		return False
	return chr

@ray.remote(max_retries=0, num_cpus=.1, resources={'head': 0.01})
def limit_pmdv_tasks(order, node_list):
	result_refs = []
	completed_chrs  = []
	for chr, id in zip(order, node_list):
		if len(result_refs) > NUM_PMDV_INSTANCES:
			ready_refs, result_refs = ray.wait(result_refs, num_returns=1)
			completed_chrs.append(ray.get(ready_refs))
		result_refs.append(manage_pmdv.options(
			name=f'manage_pmdv_{chr}').remote(str(chr), id))
	print(ray.get(result_refs))
	return completed_chrs

@ray.remote(max_retries=2, resources={'sniffles': 0.01})
def run_sniffles(bucket, chr, threads):
	# runs sniffles on merged file
	input_dir = f'/data/{chr}/'
	os.makedirs(input_dir, exist_ok=True)
	output_dir = f'{input_dir}/sniffles/'
	log_dest = f'{bucket}/sniffles/{chr}/{chr}.log'
	node_id = ray.get_runtime_context().node_id
	log_file = open_chr_log(chr, "run_sniffles")

	if not os.path.isfile(f'{input_dir}/merged.bam'):
	# download merged bam file using gcloud storage CLI
		merged_dir = f'{bucket}/bams/{chr}'
		merged_bam_files = [f'{merged_dir}/merged.bam', f'{merged_dir}/merged.bam.bai']
		download_command = ["gcloud", "alpha", "storage", "cp", input_dir]
		download_command[4:4] = merged_bam_files
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"final bam download", shell_var=False)

	chr_ref_fasta = CHR_REF_FASTA.replace("*", str(chr))
	ref_filename = os.path.basename(chr_ref_fasta)
	ref_file = f'{input_dir}/{ref_filename}'
	if not os.path.isfile(ref_file):
		# download mmi file using gcloud storage CLI
		download_command = ["gcloud", "alpha", "storage", "cp", chr_ref_fasta, input_dir]
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"fasta reference download", shell_var=False)
		close_log(log_file, log_dest)

	annot_file = f'{input_dir}/{SNIFFLES2_ANNOT["filename"]}'
	if not os.path.isfile(annot_file):
		download_command = ["wget", "-P", input_dir, SNIFFLES2_ANNOT["link"]]
		log_file = run_subprocess(download_command, log_file, log_dest, \
			"tandem-repeats annotation download", shell_var=False)
		close_log(log_file, log_dest)

	open_chr_log(chr, "sniffles")
	merged_file = f'/data/{chr}/merged.bam'
	output_file = f'{output_dir}/chr{chr}_output.vcf'

	sniffles_command = ["sniffles", "-i", merged_file, "-v", output_file,
	"--tandem-repeats", annot_file, "--allow-overwrite", "--threads", str(threads)]

	log_file = run_subprocess(sniffles_command, log_file, log_dest, \
		"sniffles run", shell_var=False)
	close_log(log_file, log_dest)

	return node_id
	
@ray.remote(max_retries=2, resources={'sniffles': 0.01})
def upload_sniffles(bucket, chr):
	# upload pmdv outputs
	input_dir = f'/data/{chr}/sniffles/'
	dest_folder = f'{bucket}/sniffles/{chr}'
	log_dest = f'{dest_folder}/{chr}.log'
	log_file = open_chr_log(chr, "upload_sniffles")
	input_file = f'{input_dir}/chr{chr}_output.vcf'

	# upload split bams using CLI
	upload_command = ["gcloud", "alpha", "storage", "cp", input_file, dest_folder]
	log_file = run_subprocess(upload_command, log_file, log_dest, \
		"sniffles upload", shell_var=False)
	close_log(log_file, log_dest)
	return chr

@ray.remote(max_retries=0, num_cpus=.1, resources={'head': 0.01})
def manage_sniffles(chr, bam_node_id, threads):
	if bam_node_id is False:
		print("bam merge for chr {chr} failed. Skipping sniffles")
		return False
	error = True
	for i in range(2):
		try:
			curr_node_id = ray.get(run_sniffles.options(
				name=f'run_sniffles_{chr}',
				num_cpus=threads, 
				resources={'sniffles': 0.01}
			).remote(BUCKET, chr, threads))

			node_strategy = NodeAffinitySchedulingStrategy(node_id=curr_node_id, soft=False)
			upload_future = upload_sniffles.options(name=f'upload_sniffles_{chr}',
				scheduling_strategy=node_strategy).remote(BUCKET, chr)
			
			ray.get(upload_future)
			error = False
			break
		except Exception as e:
			print(e)
			print(f'Sniffles for Chromosome {chr} failed a task 5 times. Retrying again max {1-i} time')
	if error:
		print(f'Sniffles for chromosome {chr} will be skipped')
		return False
	return chr

processed_fast5s = set()
batch_num = 0
merged_futures = []
times = []
names = []
with open(f'/home/ubuntu/NanoRay/flow_sim.csv', "r") as f:
	readCSV = csv.reader(f, delimiter=',')
	for row in readCSV:
		if row[3] != "Difference":
			names.append(row[1])
			times.append(float(row[3]))

names = names[:len(names)//4]
times = times[:len(times)//4]

t0 = time.time()
max_time = times[-1]

chromosomes = [str(i) for i in list(range(1, 23)) + ["X", "Y", "MT"]]
merge_chr = {}
for chr in chromosomes:
	actor = ChrBamMerger.options(name=chr).remote(BUCKET, chr)
	merge_chr[chr] = {"actor": actor, "current": []}

# Keeps running until sequencing is finished
while True:
	curr_time = time.time() - t0
	if not DONE_BOOL and curr_time > max_time:
		# done_command = ["gcloud", "alpha", "storage", 'ls', DONE_LOCATION]
		# DONE_BOOL = (subprocess.run(done_command).returncode == 0):
		DONE_BOOL = True
		print("DONE")
	# print("curr_time", curr_time)
	# if curr_time > max_time:
	# 	idx = len(times)
	# else:
	# 	idx = next(x[0] for x in enumerate(times) if x[1] > curr_time)
	idx = len(names)
	# get all fast5 files in GCS bucket and determine unprocessed ones
	# fast5_command = ["gcloud", "alpha", "storage", 'ls', FAST5_BUCKET]
	# fast5_files = subprocess.run(fast5_command, stdout=subprocess.PIPE).stdout.decode('utf-8').splitlines()
	fast5_files = names[:idx]
	unprocessed_fast5s = set(fast5_files) - processed_fast5s
	print("number unprocessed", len(unprocessed_fast5s))
	
	if len(unprocessed_fast5s) == 0 and DONE_BOOL:
		break

	while len(unprocessed_fast5s) >= MAX_FAST5s_IN_BATCH or \
		(DONE_BOOL and len(unprocessed_fast5s) > 0):

		batch_size = min(MAX_FAST5s_IN_BATCH, len(unprocessed_fast5s))
		fast5_list = [unprocessed_fast5s.pop() for _ in range(batch_size)]
		batch_name = str(batch_num)
		
		upload_futures = manage_guppy_mm2.remote(batch_name, fast5_list, chromosomes)
		upload_futures = list(upload_futures)

		for chr, upload_future in zip(chromosomes, upload_futures):
			merge_chr[chr]["current"].append(upload_future)
			finished = DONE_BOOL and len(unprocessed_fast5s) == 0
			
			# create intermediate merged bams to speed up final bam merge
			# ensure we have the required num of batches or that this is the last batch
			if len(merge_chr[chr]["current"]) == NUM_BATCHES_IN_MERGE or finished: 
				merge_batch = merge_chr[chr]["current"]
				actor = merge_chr[chr]["actor"]
				download_bams_fut = actor.get_bams.remote(*merge_batch)
				merge_bams_fut = actor.merge_bams.remote(download_bams_fut)
				upload_merged_fut = actor.upload_merged.remote(merge_bams_fut)
				merged_futures.append(upload_merged_fut)
				merge_chr[chr]["current"] = []

		processed_fast5s.update(fast5_list)
		batch_num += 1

		print("num unprocessed left in list", len(unprocessed_fast5s))

	# wait 15 seconds before seeing if new fast5 files have uploaded
	if not DONE_BOOL:
		time.sleep(15)

merged_futures = ray.get(merged_futures)
for chr in chromosomes:
	ray.kill(merge_chr[chr]["actor"])

order = PMDV_MALE if PATIENT_SEX == "M" else PMDV_FEMALE
final_merge_futures = []
for idx, chr in enumerate(order):
	first = True if idx < NUM_PMDV_INSTANCES else False
	final_merge_futures.append(manage_final_bam_merge.options(
		name=f'manage_final_bam_merge_{chr}').remote(str(chr), first))

# node_ids = ray.get(final_merge_futures)
# zipped_list = [(chr, id) for chr, id in zip(order, node_ids) if node_ids is not False]
chr_bam_dict= dict(zip(order, final_merge_futures))
pmdv_sniffles_futures = []
pmdv_sniffles_futures.append(limit_pmdv_tasks.remote(order, final_merge_futures))
for chr, threads in zip(SNIFFLES_QUEUE, SNIFFLES_RATIO):
	merge_fut = chr_bam_dict[chr]
	pmdv_sniffles_futures.append(manage_sniffles.options(
		name=f'manage_sniffles_{chr}').remote(str(chr), merge_fut, threads))
print(ray.get(pmdv_sniffles_futures))
ray.timeline(filename="/tmp/timeline.json")