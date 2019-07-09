import os
import urllib.request

directory = "../"

data_file = directory + "data.txt"
url_prefix = "ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/air_temperature/historical/"

file_number = 0

dir_list = os.listdir(directory)

with open(data_file, "r") as f:
    for line in f:
        url_postfix = line.split(" ")[-1].strip()
        file_number += 1
        if url_postfix not in dir_list:
            data_file = urllib.request.urlopen(url_prefix + url_postfix).read()
            with open(directory + url_postfix, "wb") as new_file:
                new_file.write(data_file)
            print("Saved file {}".format(file_number))
