import requests
import json
from time import sleep
import itertools
import pandas as pd
import os
import random
import string
import threading
from queue import Queue, Empty
import re
import sys

# --- Private Worker Function ---

def _run_pipeline_for_chunk(session, simulation_chunk, worker_id):
    """
    A single simulation pipeline for one chunk.
    NOW RETURNS: True if the results were successfully saved, otherwise False.
    """
    print(f"[Worker-{worker_id}] Processing new chunk of {len(simulation_chunk)} simulation(s)...")
    progress_url = submit_simulation_chunk(session, simulation_chunk, worker_id)
    
    if not progress_url:
        print(f"[Worker-{worker_id}] Pipeline failed at submission step.")
        return False

    final_alpha_ids = get_alpha_ids(session, progress_url)
    
    if not final_alpha_ids:
        print(f"[Worker-{worker_id}] Pipeline failed at retrieving alpha IDs step.")
        return False

    alpha_full_details = get_alpha_details(session, final_alpha_ids, progress_url, worker_id)
    
    if not alpha_full_details:
        # This can happen if there are no valid alphas, which isn't a hard failure,
        # but since no file is saved, we'll treat it as an incomplete task for this logic.
        print(f"[Worker-{worker_id}] Pipeline ended with no alpha details to save.")
        return False

    results_df = format_alpha_details_to_df(alpha_full_details)
    
    # The final step: save_results_to_csv now returns True or False
    was_saved = save_results_to_csv(results_df, worker_id)
    
    return was_saved


# --- REVISED AND CORRECTED WORKER FUNCTION ---
def _worker(q, session, worker_id, max_attempts_per_chunk=2):
    """
    The target function for each thread.
    - On success, waits 1s and gets a new chunk.
    - On first failure, re-queues the chunk (retry once), waits 5 mins, then continues.
    - On second failure, does NOT re-queue; treats it as completed and moves on.
    """
    while True:
        try:
            # Expect a record: {'chunk': [...], 'attempts': int}
            record = q.get_nowait()
            simulation_chunk = record.get("chunk", [])
            attempts = int(record.get("attempts", 0))

            was_successful = _run_pipeline_for_chunk(session, simulation_chunk, worker_id)

            if was_successful:
                # SUCCESS PATH
                print(f"[Worker-{worker_id}] Chunk processed successfully.")
                q.task_done()
                sleep(1)
            else:
                # FAILURE PATH
                if attempts < (max_attempts_per_chunk - 1):
                    # First failure -> re-queue for one more attempt
                    print(f"[Worker-{worker_id}] Chunk failed (attempt {attempts + 1}). Re-queueing for retry.")
                    record["attempts"] = attempts + 1
                    q.put(record)   # new task
                    q.task_done()   # mark this attempt as done
                    print(f"[Worker-{worker_id}] Entering 2-minute penalty sleep...")
                    sleep(2 * 60)
                    print(f"[Worker-{worker_id}] Penalty sleep finished. Resuming work.")
                else:
                    # Second failure -> do NOT re-queue; consider done
                    print(f"[Worker-{worker_id}] Chunk failed again (attempt {attempts + 1}). "
                          f"Max attempts reached; treating as completed and moving on.")
                    q.task_done()

        except Empty:
            # No more work
            break
        except Exception as e:
            print(f"[Worker-{worker_id}] A critical error occurred in the worker: {e}")
            # If we had a record, try not to lose it
            try:
                if 'record' in locals():
                    q.put(record)
                    q.task_done()
            except Exception:
                pass
            break


# --- Public API Functions ---
def process_alphas_in_parallel(session, simulation_list, num_threads, chunk_size, max_attempts_per_chunk=2):
    """
    Takes a pre-built list of simulation objects, chunks them, and manages multi-threaded processing.
    Retries each chunk at most once (total attempts per chunk = max_attempts_per_chunk).
    """
    simulation_queue = Queue()
    for i in range(0, len(simulation_list), chunk_size):
        chunk = simulation_list[i:i + chunk_size]
        # Enqueue with attempt counter
        simulation_queue.put({"chunk": chunk, "attempts": 0})

    print(f"Initialized queue with {simulation_queue.qsize()} chunks to be processed by {num_threads} workers.")
    
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(
            target=_worker,
            args=(simulation_queue, session, i + 1, max_attempts_per_chunk)
        )
        thread.daemon = True
        thread.start()
        threads.append(thread)

    simulation_queue.join()
    print("\n--- All alpha simulations complete. ---")


def submit_simulation_chunk(session, payload, worker_id=0):
    """Submits a single chunk of simulations to the API."""
    # This function is mostly fine, it already returns None on failure.
    if not session or not payload:
        return None
    try:
        response = session.post('https://api.worldquantbrain.com/simulations', json=payload)
        response.raise_for_status()
        progress_url = response.headers.get('Location')
        if progress_url:
            print(f"[Worker-{worker_id}] Submission successful. ID: {progress_url.split('/')[-1]}")
            return progress_url
        return None
    except requests.exceptions.RequestException as e:
        print(f"[Worker-{worker_id}] An error occurred during a simulation request: {e}")
        if e.response is not None:
            print(f"[Worker-{worker_id}] Response status: {e.response.status_code}")
            print(f"[Worker-{worker_id}] Response body: {e.response.text}")
        return None

# ... get_alpha_ids and get_alpha_details are fine as they are, returning None or [] on failure ...

def get_alpha_ids(session, progress_url):
    if not all([session, progress_url]): return None
    while True:
        try:
            progress_response = session.get(progress_url)
            progress_response.raise_for_status()
            if not progress_response.headers.get("Retry-After"): break
            sleep(float(progress_response.headers["Retry-After"]))
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while checking progress: {e}")
            return None
    try:
        parent_result = progress_response.json()
        alpha_ids = []
        child_ids = parent_result.get("children")
        if child_ids:
            for child_id in child_ids:
                child_url = f"https://api.worldquantbrain.com/simulations/{child_id}"
                child_result = session.get(child_url).json()
                if child_result.get("alpha"): alpha_ids.append(child_result.get("alpha"))
        elif parent_result.get("alpha"):
            alpha_ids.append(parent_result.get("alpha"))
        return alpha_ids
    except (json.JSONDecodeError, requests.exceptions.RequestException) as e:
        print(f"An error occurred while retrieving final alpha IDs: {e}")
        return None

def get_alpha_details(session, alpha_ids, progress_url, worker_id=0):
    if not all([session, alpha_ids]): return []
    alpha_details_list = []
    for alpha_id in alpha_ids:
        try:
            response = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}")
            response.raise_for_status()
            alpha_details_list.append(response.json())
        except requests.exceptions.RequestException as e:
            print(f"[Worker-{worker_id}] Failed to get details for alpha {alpha_id}: {e}")
    if alpha_details_list and progress_url:
        sim_id = progress_url.split('/')[-1]
        print(f"[Worker-{worker_id}] Successfully retrieved details for simulation: {sim_id}")
    return alpha_details_list

def format_alpha_details_to_df(alpha_details_list):
    # This function does not need changes.
    if not alpha_details_list:
        return pd.DataFrame()
    processed_data = []
    for alpha in alpha_details_list:
        row = {'id': alpha.get('id')}
        sim_settings = alpha.get('settings', {})
        setting_keys = ['region', 'universe', 'delay', 'decay', 'neutralization', 'truncation']
        for key in setting_keys:
            row[key] = sim_settings.get(key)
        row['code'] = alpha.get('regular', {}).get('code')
        is_stats = alpha.get('is', {})
        if is_stats:
            stat_keys = ['pnl', 'bookSize', 'longCount', 'shortCount', 'turnover', 'returns', 'drawdown', 'margin', 'sharpe', 'fitness']
            for key in stat_keys:
                row[key] = is_stats.get(key)
            for check in is_stats.get('checks', []):
                if check.get('name'):
                    row[f"check_{check.get('name')}"] = check.get('result')
        processed_data.append(row)
    return pd.DataFrame(processed_data)


def save_results_to_csv(df, worker_id=0):
    """
    Saves the DataFrame to a CSV file.
    NOW RETURNS: True on success, False on failure.
    """
    if df.empty:
        return False # Nothing to save, so it's not a success.
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    chars = string.ascii_letters + string.digits
    filename = "".join(random.choices(chars, k=6)) + ".csv"
    filepath = os.path.join(output_dir, filename)
    try:
        df.to_csv(filepath, index=False)
        print(f"[Worker-{worker_id}] Results successfully saved to: {filepath}")
        return True # Signal success
    except Exception as e:
        print(f"[Worker-{worker_id}] Failed to save DataFrame to {filepath}: {e}")
        return False # Signal failure

# ... The gather_csv_files function remains unchanged ...
def gather_csv_files(
    folder_path: str,
    stop_on_sentinel: bool = True,
    sentinel_regex: str = r"^MASS_[A-Za-z0-9]{5}\.csv$",
    use_mtime: bool = False,
    open_after_save: bool = True,
):
    """
    Combine CSV files in `folder_path` from newest to oldest, stopping when a sentinel
    file matching MASS_XXXXX.csv is found (not included). Saves a new combined file
    MASS_XXXXX.csv in the same folder and optionally opens it.

    Returns:
        The path to the saved combined file, or None if nothing was combined.
    """
    # Compile sentinel pattern (case-insensitive)
    pattern = re.compile(sentinel_regex, re.IGNORECASE)

    # Collect CSV files
    entries = []
    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                if not entry.name.lower().endswith(".csv"):
                    continue
                st = entry.stat()
                ts = st.st_mtime if use_mtime else st.st_ctime
                entries.append({"name": entry.name, "path": entry.path, "time": ts})
    except Exception:
        return None

    # Sort newest first
    entries.sort(key=lambda d: d["time"], reverse=True)

    # Combine until sentinel
    combined_df = pd.DataFrame()
    for info in entries:
        file_name = info["name"]
        file_path = info["path"]

        # Stop if a MASS_XXXXX.csv sentinel is found (do not include it)
        if stop_on_sentinel and pattern.fullmatch(file_name):
            break

        try:
            df = pd.read_csv(file_path)
        except Exception:
            continue

        combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)

    # If nothing to save, return
    if combined_df.empty:
        return None

    # Save as MASS_<5-char>.csv
    random_string = "".join(random.choices(string.ascii_letters + string.digits, k=5))
    output_file_name = f"MASS_{random_string}.csv"
    output_file_path = os.path.join(folder_path, output_file_name)

    try:
        combined_df.to_csv(output_file_path, index=False)
    except Exception:
        return None

    # Open the file (pop up) if requested
    if open_after_save:
        opened = False
        try:
            if sys.platform.startswith("win"):
                # Try os.startfile first (default handler, e.g., Excel)
                try:
                    os.startfile(output_file_path)  # type: ignore[attr-defined]
                    opened = True
                except Exception:
                    # Fallback 1: use 'start' via cmd (handles associations)
                    try:
                        subprocess.run(
                            ["cmd", "/c", "start", "", output_file_path],
                            check=False
                        )
                        opened = True
                    except Exception:
                        # Fallback 2: open via Explorer
                        try:
                            subprocess.run(["explorer", output_file_path], check=False)
                            opened = True
                        except Exception:
                            opened = False
            elif sys.platform == "darwin":
                subprocess.run(["open", output_file_path], check=False)
                opened = True
            else:
                # Linux/others
                try:
                    subprocess.run(["xdg-open", output_file_path], check=False)
                    opened = True
                except Exception:
                    try:
                        subprocess.run(["gio", "open", output_file_path], check=False)
                        opened = True
                    except Exception:
                        opened = False
        except Exception as e:
            print(f"Failed to open file automatically: {e}")
            opened = False

        if not opened:
            print(f"Saved but could not auto-open: {output_file_path}")
            print("Tip (Windows): Ensure .csv has a default app. You can also open manually or run:")
            print(f'  os.startfile(r"{output_file_path}")')

    return output_file_path
