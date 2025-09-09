import requests
import json
from time import sleep
import itertools
import pandas as pd
import os
import random
import string
import threading
from queue import Queue

# --- Private Worker Function ---
def _run_pipeline_for_chunk(session, simulation_chunk, worker_id):
    """
    A single, complete simulation pipeline for one chunk of simulations.
    This function is executed by each worker thread.
    """
    print(f"[Worker-{worker_id}] Processing new chunk of {len(simulation_chunk)} simulation(s)...")
    progress_url = submit_simulation_chunk(session, simulation_chunk, worker_id)
    
    if progress_url:
        final_alpha_ids = get_alpha_ids(session, progress_url)
        
        if final_alpha_ids:
            alpha_full_details = get_alpha_details(session, final_alpha_ids, progress_url, worker_id)
            
            if alpha_full_details:
                results_df = format_alpha_details_to_df(alpha_full_details)
                save_results_to_csv(results_df, worker_id)

def _worker(q, session, worker_id):
    """The target function for each thread. It continuously fetches and processes chunks."""
    while not q.empty():
        try:
            simulation_chunk = q.get()
            _run_pipeline_for_chunk(session, simulation_chunk, worker_id)
        finally:
            q.task_done()

# --- Public API Functions ---
def process_alphas_in_parallel(session, simulation_list, num_threads, chunk_size):
    """
    Takes a pre-built list of simulation objects, chunks them, and manages multi-threaded processing.
    """
    simulation_queue = Queue()
    for i in range(0, len(simulation_list), chunk_size):
        chunk = simulation_list[i:i + chunk_size]
        simulation_queue.put(chunk)

    print(f"Initialized queue with {simulation_queue.qsize()} chunks to be processed by {num_threads} workers.")
    
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=_worker, args=(simulation_queue, session, i + 1))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    simulation_queue.join()
    print("\n--- All alpha simulations complete. ---")

def submit_simulation_chunk(session, payload, worker_id=0):
    """Submits a single chunk of simulations to the API."""
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
    """
    Transforms the detailed JSON response into a flattened Pandas DataFrame,
    including both simulation settings and performance results.
    """
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
    if df.empty: return
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    chars = string.ascii_letters + string.digits
    filename = "".join(random.choices(chars, k=6)) + ".csv"
    filepath = os.path.join(output_dir, filename)
    try:
        df.to_csv(filepath, index=False)
        print(f"[Worker-{worker_id}] Results successfully saved to: {filepath}")
    except Exception as e:
        print(f"[Worker-{worker_id}] Failed to save DataFrame to {filepath}: {e}")

