import sys
import os
import urllib
import zipfile
import contextlib

def unzip(source, target):
    with contextlib.closing(zipfile.ZipFile(source , "r")) as z:
        z.extractall(target)
        print "Extracted : " + source +  " to: " + target

sys.path.append("..")

DownloadBaseSmall = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'
DownloadBaseLarge = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'

datasets_path = "MovieLensData"
complete_dataset_path = os.path.join(datasets_path, 'ml-latest.zip')
small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')
force = False
if not os.path.exists(small_dataset_path) or force:
    print("File ml-latest-small doesn't exists. Downloading Now!!")
    urllib.urlretrieve (DownloadBaseSmall, small_dataset_path)
else:
    print("File ml-latest-small exists. Not Downlaoding!!")

if not os.path.exists(complete_dataset_path) or force:
    print("File ml-latest doesn't exists. Downloading Now!!")
    urllib.urlretrieve (DownloadBaseLarge, complete_dataset_path)
else:
    print("File ml-latest exists. Not Downlaoding!!")

complete_dataset_unzipped_path = os.path.join(datasets_path,'ml-latest')
small_dataset_unzipped_path = os.path.join(datasets_path,'ml-latest-small')
force = False
if not os.path.exists(complete_dataset_unzipped_path) or force:
    print("File ml-latest not unzipped. Unzipping Now!!")
    unzip(complete_dataset_path,datasets_path)
else:
    print("File ml-latest already unzipped. Not unzipping!!")

if not os.path.exists(small_dataset_unzipped_path) or force:
    print("File ml-latest-small not unzipped. Unzipping Now!!")
    unzip(small_dataset_path,datasets_path)
else:
    print("File ml-latest-small already unzipped. Not unzipping!!")
