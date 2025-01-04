import logging
import os
from dotenv import load_dotenv
from firebird.FirebirdHandler import FirebirdHandler
from sftp.SFTPHandler import SFTPHandler
from utils.Logger import Logger
from utils.errors import FirebirdConnectionError, FirebirdQueryError

import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk

# Function to save and fetch scheduled tasks from the database
def save_task_to_db(task_details):
    # Logic to save task details to the database
    pass

def fetch_tasks_from_db():
    # Logic to fetch scheduled tasks from the database
    return [
        {"id": 1, "name": "Task 1", "status": "Completed"},
        {"id": 2, "name": "Task 2", "status": "Pending"},
    ]

def job(query, output_file, remote_path):
    """
    Job to run the process of fetching data, saving to a file, and uploading it.
    """
    firebird_config = {
        "host": os.getenv("FIREBIRD_HOST"),
        "port": int(os.getenv("FIREBIRD_PORT")),
        "database": os.getenv("FIREBIRD_DATABASE"),
        "user": os.getenv("FIREBIRD_USER"),
        "password": os.getenv("FIREBIRD_PASSWORD")
    }
    sftp_config = {
        "host": os.getenv("SFTP_HOST"),
        "username": os.getenv("SFTP_USER"),
        "password": os.getenv("SFTP_PASS"),
        "port": int(os.getenv("SFTP_PORT", 22))
    }

    db_handler = FirebirdHandler(**firebird_config)
    sftp_handler = SFTPHandler(**sftp_config)

    try:
        db_handler.connect()
        db_handler.execute_query_to_csv(query, output_file)
        sftp_handler.connect()
        sftp_handler.upload_file(output_file, remote_path)
    finally:
        db_handler.close()
        sftp_handler.close_connection()

def open_gui():
    def start_job():
        try:
            query = query_text.get("1.0", "end-1c")
            output_file = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
            remote_path = remote_path_entry.get()

            if not output_file:
                messagebox.showerror("Error", "Please select a file to save the output.")
                return

            job(query, output_file, remote_path)

            task_details = {
                "query": query,
                "output_file": output_file,
                "remote_path": remote_path,
                "status": "Scheduled"
            }

            save_task_to_db(task_details)
            update_task_list()

            messagebox.showinfo("Success", "Job scheduled successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {e}")

    def update_task_list():
        tasks = fetch_tasks_from_db()
        for row in task_list.get_children():
            task_list.delete(row)
        for task in tasks:
            task_list.insert("", "end", values=(task["id"], task["name"], task["status"]))

    root = tk.Tk()
    root.title("Scheduled Tasks Manager")

    # Task configuration frame
    config_frame = tk.Frame(root)
    config_frame.pack(padx=10, pady=5, fill=tk.X)

    tk.Label(config_frame, text="SQL Query:").grid(row=0, column=0, padx=10, pady=5)
    query_text = tk.Text(config_frame, height=5, width=50)
    query_text.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="Remote Path:").grid(row=1, column=0, padx=10, pady=5)
    remote_path_entry = tk.Entry(config_frame, width=50)
    remote_path_entry.grid(row=1, column=1, padx=10, pady=5)

    tk.Button(config_frame, text="Schedule Task", command=start_job).grid(row=2, column=0, columnspan=2, pady=10)

    # Task list frame
    list_frame = tk.Frame(root)
    list_frame.pack(padx=10, pady=5, fill=tk.BOTH, expand=True)

    tk.Label(list_frame, text="Scheduled Tasks:").pack(anchor="w", padx=10, pady=5)

    columns = ("id", "name", "status")
    task_list = ttk.Treeview(list_frame, columns=columns, show="headings")
    task_list.heading("id", text="ID")
    task_list.heading("name", text="Task Name")
    task_list.heading("status", text="Status")
    task_list.pack(fill=tk.BOTH, expand=True)

    update_task_list()

    root.mainloop()

if __name__ == "__main__":
    load_dotenv()
    Logger.setup_logging()
    open_gui()
