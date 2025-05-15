#!/usr/bin/env python3
from pysradb.sraweb import SRAweb
from joblib import Parallel, delayed
import subprocess
import glob
import os

#################configs#################
mode = "SRR_download"
jobs = 10
#if wanna input project name
projectname = "SRP098789"
#if wanna input SRR list
srr_list = ["SRR5227288", "SRR5227289", "SRR5227290"]

#################download#################
db = SRAweb()
if mode  == "SRP_download":
    projectname = projectname
    out_dir = "RawSRA/SRP_download"
    dfs = db.sra_metadata(projectname, detailed=True)

if mode == "SRR_download":
    out_dir = "RawSRA/SRR_download"
    srr_list = srr_list
    df_SRR = db.sra_metadata(srr_list, detailed=True)
    dfs = df_SRR[df_SRR["run_accession"].isin(srr_list)]

def single_download(df_single, out_dir=out_dir):
    db.download(df=df_single, skip_confirmation=True, out_dir=out_dir)

print(dfs)
Parallel(n_jobs=jobs)(delayed(single_download)(df_x) for df_x in [dfs])


#################SRA->Fastq#################
sra_files = glob.glob("RawSRA/**/*.sra", recursive=True)
output_dir = "FastqData"
os.makedirs(output_dir, exist_ok=True)

def convert_to_fastq(sra_path, output_dir):
    try:
        cmd = [
            "fasterq-dump",
            sra_path,
            "-O", output_dir,
            "--split-files",  # split paired-end reads
            "-e", "4"  # threads per task
        ]
        subprocess.run(cmd, check=True)
        print(f"Converted: {os.path.basename(sra_path)}")
    except subprocess.CalledProcessError:
        print(f"Failed: {os.path.basename(sra_path)}")

Parallel(n_jobs=jobs)(
    delayed(convert_to_fastq)(sra, output_dir) for sra in sra_files
)