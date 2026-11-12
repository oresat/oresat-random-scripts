"""
ECE 506: OreSat Reaction Wheel Monitor
Author: Chris Kane-Pardy
Date: Fall 2025

DESCRIPTION:
    Primary interface for monitoring the OreSat Reaction Wheel testbench.

    WORKFLOW:
    1. Select Mode.
    2. [Standard] Pre-Test Zero (Tare).
    3. [Standard] Mount Check.
    4. Live Monitoring / Data Collection.
    5. [Standard] Drift Check.
    6. Post-Test Analysis.
"""

from labjack import ljm
import time
import csv
import os
import statistics
import sys
import matplotlib.pyplot as plt
import pandas as pd
from collections import deque

# Try to import msvcrt for Windows 'q' keybind
try:
    import msvcrt
    WINDOWS_OS = True
except ImportError:
    WINDOWS_OS = False

# ==========================================
# SECTION 1: SYSTEM CONFIGURATION
# ==========================================

# SENSITIVITY CONSTANT (Volts per Nm)
SENSITIVITY_V_PER_NM = 10.0389

# SIGNAL PROCESSING SETTINGS
SMOOTHING_WINDOW = 50

# LABJACK SETTINGS (DIFFERENTIAL MODE)
TARGET_CHANNEL = "AIN2"
# NEGATIVE_CH_INDEX = 199 # Single-Ended (GND)
NEGATIVE_CH_INDEX = 3     # Differential (AIN2 - AIN3)
INPUT_RANGE = 10.0        # +/- 10V range

# ==========================================
# SECTION 2: HARDWARE INTERFACE
# ==========================================

def SetupLabJack():
    # Force close any lingering connections from crashed scripts
    try:
        ljm.closeAll()
    except:
        pass

    try:
        # Open the first found T7
        handle = ljm.openS("T7", "ANY", "ANY")
        info = ljm.getHandleInfo(handle)
        print(f" >> Connected to LabJack T7 (Serial: {info[2]})")
        
        # Configure Channel AIN1 for Differential Mode (AIN1 - AIN2)
        ljm.eWriteName(handle, f"{TARGET_CHANNEL}_NEGATIVE_CH", NEGATIVE_CH_INDEX)
        ljm.eWriteName(handle, f"{TARGET_CHANNEL}_RANGE", INPUT_RANGE)
        ljm.eWriteName(handle, f"{TARGET_CHANNEL}_RESOLUTION_INDEX", 8) # Using 8 for speed/precision balance
        
        print(f" >> Configured Differential Input: AIN1 (Pos) - AIN{NEGATIVE_CH_INDEX} (Neg)")
        return handle
    except ljm.LJMError as e:
        print(f" !! ERROR: Could not connect to LabJack T7.")
        print(f"    Error Code: {e}")
        print("    Check USB connection and ensure no other software (Kipling/LJLog) is using the device.")
        sys.exit()

def GetReading(handle, samples=5000):
    readings = []
    for _ in range(samples):
        readings.append(ljm.eReadName(handle, TARGET_CHANNEL))
    return statistics.mean(readings)

# ==========================================
# SECTION 3: UTILITY FUNCTIONS
# ==========================================

def CheckForKeyPress():
    if WINDOWS_OS:
        if msvcrt.kbhit():
            key = msvcrt.getch()
            if key.lower() == b'q':
                return True
    return False

def WaitForEnter(prompt):
    input(f"\n >> ACTION REQUIRED: {prompt}\n    (Press Enter to confirm...)")

def PlotTestResults(filename):
    try:
        if not os.path.exists(filename):
            print(" !! Error: Log file not found.")
            return

        print(f"\n >> Generating Post-Test Graph from {filename}...")
        df = pd.read_csv(filename)
        
        if len(df) < 2:
            print(" !! Not enough data to plot.")
            return

        plt.figure(figsize=(12, 7))
        
        plt.plot(df['Time_Sec'], df['Raw_Torque_mNm'], color='gray', linewidth=0.8, alpha=0.4, label='Raw Signal')
        plt.plot(df['Time_Sec'], df['Smoothed_Torque_mNm'], color='#049910', linewidth=2, label=f'Smoothed (Avg {SMOOTHING_WINDOW})')
        
        max_torque = df['Smoothed_Torque_mNm'].abs().max()
        plt.title(f"Reaction Wheel Test Results\nMax Torque: {max_torque:.3f} mNm", fontsize=14, fontweight='bold')
        plt.xlabel("Time (seconds)", fontsize=11)
        plt.ylabel("Torque (mNm)", fontsize=11)
        plt.axhline(0, color='black', linewidth=0.8, linestyle='-', alpha=0.5) 
        plt.grid(True, linestyle=':', alpha=0.6)
        plt.legend(loc='upper right')
        plt.tight_layout()
        
        image_filename = filename.replace(".csv", ".png")
        plt.savefig(image_filename)
        print(f" >> Graph saved to: {image_filename}")
        
        print(" >> Displaying graph... (Close window to proceed)")
        plt.show(block=True)
        
    except Exception as e:
        print(f" !! Error generating graph: {e}")

# ==========================================
# SECTION 4: MAIN EXECUTION
# ==========================================

def Main():
    print("\n" + "="*60)
    print("   ORESAT REACTION WHEEL TORQUE MONITOR")
    print(f"   Sensitivity: {SENSITIVITY_V_PER_NM:.4f} V/Nm")
    print("="*60)
    
    print("\nSelect Operation Mode:")
    print("   [1] Standard Procedure (Full Zeroing & Drift Checks)")
    print("   [2] Quick Start (Bypass Checks - Live Monitor Only)")
    
    mode_choice = input("\n>> Enter Choice (1/2): ").strip()
    
    handle = SetupLabJack()
    
    # --- PHASE 1: PRE-TEST ZERO ---
    if mode_choice == '1':
        print("\n[PHASE 1] PRE-TEST ZERO CHECK")
        WaitForEnter("Ensure sensor is EMPTY/CLEAR (No Satellite).")
        print("    Acquiring Zero Baseline...", end="", flush=True)
        readings = []
        for _ in range(5000): 
            readings.append(ljm.eReadName(handle, TARGET_CHANNEL))
        v_zero_pre = statistics.mean(readings)
        print(f" Done. (Base: {v_zero_pre:.5f} V)")
        
        # --- PHASE 2: MOUNT CHECK ---
        print("\n[PHASE 2] MOUNT CHECK")
        WaitForEnter("Mount Satellite.")
        print("    Measuring Static Load...", end="", flush=True)
        readings = []
        for _ in range(5000):
            readings.append(ljm.eReadName(handle, TARGET_CHANNEL))
        v_mounted = statistics.mean(readings)
        static_torque_mnm = ((v_mounted - v_zero_pre) / SENSITIVITY_V_PER_NM) * 1000.0
        print(f" Done.")
        print(f"    Static Torque Change: {static_torque_mnm:+.3f} mNm")
    
    else:
        # QUICK START MODE
        print("\n[QUICK START] Bypassing manual zero checks.")
        print("    Acquiring current voltage as zero reference...", end="", flush=True)
        # Take a quick reading to establish relative zero
        readings = []
        for _ in range(500): 
            readings.append(ljm.eReadName(handle, TARGET_CHANNEL))
        v_zero_pre = statistics.mean(readings)
        print(f" Done. (Base: {v_zero_pre:.5f} V)")
        static_torque_mnm = 0.0 # Unknown in this mode

    # --- PHASE 3: LIVE MONITORING ---
    if mode_choice == '1':
        print("\n[PHASE 3] LIVE MONITORING")
        WaitForEnter("Start Reaction Wheel Test.")
    else:
        print("\n[LIVE MONITORING] Starting data log immediately...")
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    filename = f"TestLog_{timestamp}.csv"
    
    print(f"\n >> STARTED. Logging to: {filename}")
    if WINDOWS_OS:
        print(" >> Press 'q' to STOP.\n")
    else:
        print(" >> Press CTRL+C to STOP.\n")

    print("-" * 75)
    print(f"{'Time (s)':<10} | {'Voltage (V)':<12} | {'Raw (mNm)':<12} | {'Smooth (mNm)':<12}")
    print("-" * 75)

    start_time = time.time()
    max_torque_observed = 0.0
    torque_buffer = deque(maxlen=SMOOTHING_WINDOW)

    f = open(filename, mode='w', newline='')
    writer = csv.writer(f)
    writer.writerow(["Time_Sec", "Raw_Voltage_V", "Raw_Torque_mNm", "Smoothed_Torque_mNm"])

    try:
        while True:
            if CheckForKeyPress():
                print("\n\n >> STOPPING TEST (User Request)...")
                break

            v_curr = GetReading(handle, samples=10)
            
            delta_v = v_curr - v_zero_pre
            torque_nm = delta_v / SENSITIVITY_V_PER_NM
            torque_mnm = torque_nm * 1000.0
            
            torque_buffer.append(torque_mnm)
            smooth_mnm = sum(torque_buffer) / len(torque_buffer)

            if abs(smooth_mnm) > abs(max_torque_observed):
                max_torque_observed = smooth_mnm
            
            elapsed = time.time() - start_time
            writer.writerow([round(elapsed, 4), round(v_curr, 6), round(torque_mnm, 6), round(smooth_mnm, 6)])
            
            print(f"\r{elapsed:<10.2f} | {v_curr:<12.5f} | {torque_mnm:<12.3f} | {smooth_mnm:<12.3f}   ", end="")
            sys.stdout.flush()

    except KeyboardInterrupt:
        print("\n\n >> STOPPING TEST (Ctrl+C Detected)...")
    
    finally:
        f.close()
        print(f"\n >> Log file saved: {filename}")

    # --- PHASE 4: GRAPH ---
    PlotTestResults(filename)

    # --- PHASE 5: DRIFT CHECK (Standard Mode Only) ---
    if mode_choice == '1':
        print("\n[PHASE 4] POST-TEST DRIFT CHECK")
        WaitForEnter("Remove Satellite (Clear Sensor).")
        
        print("    Acquiring Final Zero...", end="", flush=True)
        readings = []
        for _ in range(5000):
            readings.append(ljm.eReadName(handle, TARGET_CHANNEL))
        v_zero_post = statistics.mean(readings)
        print(f" Done. (Base: {v_zero_post:.5f} V)")

        drift_v = v_zero_post - v_zero_pre
        drift_mnm = ((drift_v / SENSITIVITY_V_PER_NM) * 1000.0)
        
        print("\n" + "="*60)
        print("   TEST SESSION SUMMARY")
        print("="*60)
        print(f"   Static Load (Mount):  {static_torque_mnm:+.3f} mNm")
        print(f"   Max Torque (Smooth):  {max_torque_observed:.3f} mNm")
        print(f"   Zero Drift (Torque):  {drift_mnm:+.3f} mNm")
        
        if abs(drift_mnm) < 5.0:
            print("   STATUS: PASS (Sensor was stable)")
        else:
            print("   STATUS: WARNING (Significant Zero Shift detected)")
        print("="*60 + "\n")
    else:
        print("\n" + "="*60)
        print("   QUICK SESSION SUMMARY")
        print("="*60)
        print(f"   Max Torque (Smooth):  {max_torque_observed:.3f} mNm")
        print("   (Drift check bypassed)")
        print("="*60 + "\n")

    ljm.close(handle)

if __name__ == "__main__":
    Main()
