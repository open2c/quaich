import argparse
from coolpuppy.lib import io as cpio

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", required=True, nargs="+")
parser.add_argument("-o", "--output", required=True)

args = parser.parse_args()

pileups = cpio.load_pileup_df_list(args.input)
cpio.save_pileup_df(args.output, pileups)
