import sys
sys.stdout.reconfigure(line_buffering=True)

print("STEP 1: main.py started", flush=True)

try:
    print("STEP 2: importing bot...", flush=True)
    from dex_sniper_bot import main
    print("STEP 3: import successful", flush=True)
    main()
except Exception as e:
    print(f"ERROR: {e}", flush=True)
    import traceback
    traceback.print_exc()
    sys.stdout.flush()
    # Keep alive so logs are visible
    import time
    time.sleep(300)
