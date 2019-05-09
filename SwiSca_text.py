# -*- coding: utf-8 -*-

import pandas as pd
import glob
from pandas.io.json import json_normalize
import argparse

parser = argparse.ArgumentParser("archive")
parser.add_argument("archive_dir", nargs='?', default='./', action="store",
                help="a directory where results are stored")
args = parser.parse_args()
filepath = './'+args.archive_dir+'/tweetdata/'
#filepath = os.path.abspath("./tweetdata")
output_file = filepath+'tweets_output.csv'
files = [x for y in [glob.glob(e, recursive = True) for e in [filepath+'/**/*.json',filepath+'/*.jsonl',filepath+'/*.gz']] for x in y]

df_t = pd.DataFrame()
for x in files:
        if x.endswith(("json","jsonl","gz")):
            try:
                df = pd.read_json(x, lines = True, compression = "infer")
            except:
                continue
            try:
                df["screen_name"] = json_normalize(df["user"])["screen_name"]
                df["country"] = ["None" if str(x["place"])=="None" else json_normalize(x["place"])["country"].values[0] for i,x in df.iterrows()]
                df_t = df_t.append(df[["screen_name","country","text"]])
            except:
                continue
df_t.to_csv(output_file)
