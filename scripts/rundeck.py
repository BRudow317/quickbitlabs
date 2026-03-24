#!/usr/bin/env python3
import os
import sys
import subprocess
import venv

# --- CONFIGURATION ---
# Using an absolute path is safer for Rundeck; otherwise, it uses the current working dir.
VENV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "RundeckBaseVenv")
CONFIG_PATH = "/stage/rundeck-scripts/config.dat"
ENV_NAME = "dev01"

def setup_venv():
    """Creates a persistent virtual environment if it doesn't exist."""
    if not os.path.exists(VENV_DIR):
        print(f"--- Creating persistent venv: {VENV_DIR} ---")
        # 'prompt' sets the name you see when the venv is activated
        venv.create(VENV_DIR, with_pip=True, prompt="RundeckBaseVenv")

    venv_python = os.path.join(VENV_DIR, "bin", "python3")
    
    # Check for oracledb and install/update if missing
    try:
        subprocess.check_call([venv_python, "-c", "import oracledb"], 
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        print("--- oracledb missing. Installing to RundeckBaseVenv... ---")
        subprocess.check_call([venv_python, "-m", "pip", "install", "--upgrade", "pip"])
        subprocess.check_call([venv_python, "-m", "pip", "install", "oracledb"])

    return venv_python

def get_db_credentials(env_name):
    """Parses shell config.dat and handles indirect expansion."""
    cmd = f"source {CONFIG_PATH} {env_name} && env"
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, executable="/bin/bash")
    
    raw_env = {}
    for line in proc.stdout:
        line = line.decode("utf-8").strip()
        if "=" in line:
            k, v = line.split("=", 1)
            raw_env[k] = v
            
    try:
        return {
            "user": raw_env[raw_env['perf_username']],
            "pwd":  raw_env[raw_env['perf_password']],
            "dsn":  raw_env[raw_env['perf_sid']]
        }
    except KeyError as e:
        print(f"Error: Missing config key {e}")
        sys.exit(1)

def run_job(proc_type, file_name):
    """Core Database Logic."""
    import oracledb
    creds = get_db_credentials(ENV_NAME)
    
    try:
        # thin=True is default for newer oracledb; doesn't require Oracle Instant Client
        conn = oracledb.connect(user=creds["user"], password=creds["pwd"], dsn=creds["dsn"])
        cursor = conn.cursor()
        
        if proc_type == "enrollment":
            v_ret = cursor.callfunc("perf.ERM_ENROLLMENT_PKG.f_complete_run", str, [file_name])
        elif proc_type == "feedback":
            v_ret = cursor.callfunc("perf.perf_feedback_pkg.f_process_feedback", str)
        elif proc_type == "validation":
            v_ret = cursor.callfunc("perf.ERM_VALIDATION_PKG.f_process_validation_file", str, [file_name])
        else:
            raise ValueError(f"Invalid job type: {proc_type}")

        print(f"Result: {'SUCCESS' if v_ret == '0' else 'FAILURE'} (Code: {v_ret})")
        return v_ret == "0"

    except Exception as e:
        print(f"Database Error: {e}")
        return False
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 script.py [enrollment|feedback|validation] [filename]")
        sys.exit(1)

    # Check if we are already inside the venv
    if "VIRTUAL_ENV" not in os.environ:
        job_type = sys.argv[1].lower()
        file_arg = sys.argv[2] if len(sys.argv) > 2 else ""
        
        # Ensure venv exists and is ready
        python_bin = setup_venv()
        
        # Run self inside the venv
        result = subprocess.run([python_bin, __file__, "INTERNAL_EXEC", job_type, file_arg])
        # Note: We NO LONGER delete the venv here. It persists for the next run.
        sys.exit(result.returncode)

    # Internal execution logic
    if sys.argv[1] == "INTERNAL_EXEC":
        success = run_job(sys.argv[2], sys.argv[3])
        sys.exit(0 if success else 1)