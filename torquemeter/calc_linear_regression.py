"""
ECE 506: Torque Sensor Calibration Analysis (Absolute Physics Mode)
Author: Chris Kane-Pardy
Date: Fall 2025


METHODOLOGY:
1. Uses Arm Length & Mass to calculate Base Torque (Pre-load).
2. X-Axis = Added Torque (Weights) + Base Torque (Fixture).
3. Removes points deviating > 2 sigma from the global trend.
4. Fits a line to the Absolute Torque vs Absolute Voltage.

OUTPUTS:
- Scatter Plot: Absolute Total Torque vs Sensor Voltage.
- Global Fit Line: The master calibration curve.
- Histogram: Distribution of sensor noise.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
from scipy import stats

# Configuration
FILENAME = "calibrationDataSlope.csv"
GRAVITY = 9.80665

# --- FIXTURE DEFINITIONS ---
# Used to calculate the Base Torque (the "Pre-load" from the arm weight)
OLD_ARM_METERS = 0.13081 
OLD_FIXTURE_MASS_G = 295.5     

NEW_ARM_METERS = 0.045847 
NEW_FIXTURE_MASS_G = 176.69    

def GetFixtureBaseTorque(row):
    """
    Calculate the base torque (pre-load) applied by the fixture itself.
    """
    length = row['Lever_Arm_m']
    
    # Using a small tolerance for float comparison
    if abs(length - OLD_ARM_METERS) < 1e-4:
        mass_g = OLD_FIXTURE_MASS_G
    elif abs(length - NEW_ARM_METERS) < 1e-4:
        mass_g = NEW_FIXTURE_MASS_G
    else:
        # Default fallback if arm length doesn't match known fixtures
        mass_g = 0 
        
    base_torque = (mass_g / 1000.0) * GRAVITY * length
    return base_torque

def LoadAndCleanData(filename):
    """
    Loads CSV, calculates Absolute Torque.
    """
    try:
        df = pd.read_csv(filename)
        
        cols = ['Added_Mass_g', 'Sensor_Voltage_V', 'Added_Torque_Nm', 'Lever_Arm_m', 'Resting_Voltage_V']
        for c in cols:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        
        df = df.dropna(subset=cols)
        
        # --- ABSOLUTE TORQUE CALCULATION ---
        # 1. Calculate Base Torque (Fixture Weight)
        df['Base_Torque_Nm'] = df.apply(GetFixtureBaseTorque, axis=1)
        
        # 2. Calculate TOTAL Absolute Torque
        # X-Axis is now the true physical torque on the sensor
        df['Analysis_Torque_Nm'] = df['Added_Torque_Nm'] + df['Base_Torque_Nm']

        # Group by Session ID for coloring/tracking
        df['Session_ID'] = df['Resting_Voltage_V']
        
        return df
    except FileNotFoundError:
        print(f"Error: Could not find '{filename}'.")
        return None

def RemoveOutliers(df):
    """
    Filter data by removing points outside 2 Standard Deviations (95% Confidence).
    Uses the GLOBAL trend for outlier detection since physics should be consistent.
    """
    print("\n--- OUTLIER REMOVAL (> 2 Sigma) ---")
    
    # 1. Fit Global Line (Physical Prediction)
    slope, intercept, _, _, _ = stats.linregress(df['Analysis_Torque_Nm'], df['Sensor_Voltage_V'])
    
    # 2. Calculate Residuals
    predicted_y = slope * df['Analysis_Torque_Nm'] + intercept
    residuals = df['Sensor_Voltage_V'] - predicted_y
    
    # 3. Determine Threshold
    sigma = residuals.std()
    threshold = 2 * sigma
    
    # 4. Filter
    is_valid = np.abs(residuals) <= threshold
    df_clean = df[is_valid].copy()
    
    dropped = len(df) - len(df_clean)
    print(f" >> Global Sigma: {sigma:.5f}")
    print(f" >> TOTAL REMOVED: {dropped} outliers from dataset.")
    
    return df_clean

def CalculateGlobalStats(df):
    """
    Calculates noise statistics across the entire dataset.
    """
    # Group by specific torque steps (rounded) to find noise at each step
    df['Group_Torque'] = df['Analysis_Torque_Nm'].round(4)
    
    grouped = df.groupby('Group_Torque').agg(
        StdDev_Voltage=('Sensor_Voltage_V', 'std')
    ).reset_index()
    
    grouped['StdDev_Voltage'] = grouped['StdDev_Voltage'].fillna(0)
    return grouped

def PlotStatistics(rawDf, statsDf):
    """
    Generates the Final Calibration Plot
    """

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12), sharex=False)
    
    # --- PLOT 1: CALIBRATION CURVE ---
    unique_sessions = sorted(rawDf['Session_ID'].unique())
    colors = cm.hsv(np.linspace(0, 0.9, len(unique_sessions)))
    
    print(f"\nProcessing {len(unique_sessions)} sessions...")

    # Plot Scatter Points
    # Trick: We only label the first one "Measured Data" to keep the legend clean
    for i, session_id in enumerate(unique_sessions):
        s_data = rawDf[rawDf['Session_ID'] == session_id]
        label = "Measurements" if i == 0 else "_nolegend_"
        ax1.scatter(s_data['Analysis_Torque_Nm'], s_data['Sensor_Voltage_V'], 
                   color=colors[i], s=25, alpha=0.6, label=label)

    # --- GLOBAL FIT CALCULATION ---
    g_slope, g_int, g_r, g_p, g_std_err = stats.linregress(rawDf['Analysis_Torque_Nm'], rawDf['Sensor_Voltage_V'])
    
    # Plot Global Line
    g_x = np.linspace(0, rawDf['Analysis_Torque_Nm'].max() * 1.05, 100)
    g_y = g_slope * g_x + g_int
    
    # Linear Regression Slope
    ax1.plot(g_x, g_y, color="#000000", linestyle='--', linewidth=2.5, label='Linear Fit', zorder=10)

    # Statistics Box
    stats_text = (f"$\\bf{{Sensitivity: {g_slope:.4f}\\ V/Nm}}$\n"
                  f"Linearity ($R^2$): {g_r**2:.5f}\n"
                  f"Abs. Zero Offset: {g_int:.4f} V")
    
    ax1.text(0.03, 0.97, stats_text, transform=ax1.transAxes, fontsize=11,
             verticalalignment='top', bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.95, edgecolor='#cccccc'))

    ax1.set_ylabel("Sensor Output (V)", fontsize=10, fontweight='medium')
    ax1.set_xlabel("Absolute Torque (Nm)", fontsize=10, fontweight='medium')
    ax1.set_title(f"Torque Sensor Calibration Curve", fontsize=14, fontweight='bold')
    
    # Grid & Legend
    ax1.grid(True, linestyle=':', alpha=0.6)
    ax1.legend(loc='lower right', frameon=True, framealpha=0.9, fontsize=11)

    # --- PLOT 2: STDev HISTOGRAM ---
    mu, std = stats.norm.fit(statsDf['StdDev_Voltage'])
    
    # Histogram Bars
    n, bins, patches = ax2.hist(statsDf['StdDev_Voltage'], bins=40, density=True, 
                                color="#7E92FF", alpha=0.75, edgecolor='white', linewidth=0.5, label='Measured Noise')
    
    # Bell Curve
    xmin, xmax = ax2.get_xlim()
    x = np.linspace(xmin, xmax, 100)
    p = stats.norm.pdf(x, mu, std)
    ax2.plot(x, p, color="#000000", linewidth=2, label=f'Normal Dist. ($\\sigma$={std:.4f})')
    
    # Mean Line
    ax2.axvline(mu, color="#000000", linestyle='--', linewidth=2, label=f'Mean: {mu:.4f} V')

    ax2.set_xlabel("Standard Deviation of Signal (V)", fontsize=10, fontweight='medium')
    ax2.set_ylabel("Probability Density", fontsize=10, fontweight='medium')
    ax2.set_title("Standard Deviation", fontsize=14, fontweight='bold')
    ax2.grid(True, linestyle=':', alpha=0.6)
    ax2.legend(loc='upper right', frameon=True, framealpha=0.9)

    plt.tight_layout()
    print(f"\n>> FINAL SENSITIVITY: {g_slope:.5f} V/Nm")
    print(f">> LINEARITY (R^2):   {g_r**2:.5f}")
    plt.show()

def Main():
    print("--- ECE 506 Final Calibration Analysis ---")
    df = LoadAndCleanData(FILENAME)
    if df is None: return
    if len(df) < 5: 
        print("Insufficient data."); return

    df_clean = RemoveOutliers(df)
    statsDf = CalculateGlobalStats(df_clean)
    PlotStatistics(df_clean, statsDf)

if __name__ == "__main__":
    Main()
