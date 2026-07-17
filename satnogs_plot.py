import pandas as pd 
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates
import argparse

# to run:  python satnogs_plot.py beacons.csv --variable 'battery_1_pack_1_vbatt (mV)'
# last parameter is the variable to plot.  Check headings of beacons.csv file for names.

def main():
    parser = argparse.ArgumentParser(description="Script to plot SatNogs data")

    parser.add_argument("input_file", help="Path to the input file.")
    parser.add_argument("--variable", help="Variable name to plot")
    args = parser.parse_args()

    df = pd.read_csv(args.input_file)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    var = args.variable

    fig = plt.plot(df['timestamp'], df[var], marker='.', linestyle='')
    plt.xticks(rotation=45)

    plt.ylabel(var)
    plt.show()

if __name__ == "__main__":
    main()