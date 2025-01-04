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
import re

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

def job(task_name, query, output_file, remote_path, sftp_host, sftp_user, sftp_pass):
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
        "host": sftp_host,
        "username": sftp_user,
        "password": sftp_pass,
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
    def validate_inputs():
        errors = []
        if not task_name_entry.get():
            errors.append("Task name is required.")

        if not output_file_entry.get():
            errors.append("Output file name is required.")

        if not sftp_host_entry.get() or not sftp_user_entry.get() or not sftp_pass_entry.get():
            errors.append("SFTP details (host, user, and password) are required.")

        cron_expression = cron_entry.get()
        if not cron_expression:
            errors.append("Cron expression is required.")
        else:
            cron_regex = r"^((\*|[0-5]?\d)\s+(\*|1?\d|2[0-3])\s+(\*|[1-9]|[12]\d|3[01])\s+(\*|[1-9]|1[0-2])\s+(\*|[0-6]))$"
            if not re.match(cron_regex, cron_expression):
                errors.append("Invalid cron expression format.")

        return errors

    def start_job():
        try:
            errors = validate_inputs()
            if errors:
                messagebox.showerror("Input Validation Error", "\n".join(errors))
                return

            task_name = task_name_entry.get()
            query = query_text.get("1.0", "end-1c")
            output_file = output_file_entry.get()
            remote_path = remote_path_entry.get()
            sftp_host = sftp_host_entry.get()
            sftp_user = sftp_user_entry.get()
            sftp_pass = sftp_pass_entry.get()
            cron_expression = cron_entry.get()

            task_details = {
                "name": task_name,
                "query": query,
                "output_file": output_file,
                "remote_path": remote_path,
                "sftp_host": sftp_host,
                "sftp_user": sftp_user,
                "cron_expression": cron_expression,
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

    tk.Label(config_frame, text="Task Name:").grid(row=0, column=0, padx=10, pady=5)
    task_name_entry = tk.Entry(config_frame, width=50)
    task_name_entry.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="SQL Query:").grid(row=1, column=0, padx=10, pady=5)
    query_text = tk.Text(config_frame, height=5, width=50)
    query_text.grid(row=1, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="Output File Name:").grid(row=2, column=0, padx=10, pady=5)
    output_file_entry = tk.Entry(config_frame, width=50)
    output_file_entry.grid(row=2, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="Remote Path:").grid(row=3, column=0, padx=10, pady=5)
    remote_path_entry = tk.Entry(config_frame, width=50)
    remote_path_entry.grid(row=3, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="SFTP Host:").grid(row=4, column=0, padx=10, pady=5)
    sftp_host_entry = tk.Entry(config_frame, width=50)
    sftp_host_entry.grid(row=4, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="SFTP User:").grid(row=5, column=0, padx=10, pady=5)
    sftp_user_entry = tk.Entry(config_frame, width=50)
    sftp_user_entry.grid(row=5, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="SFTP Password:").grid(row=6, column=0, padx=10, pady=5)
    sftp_pass_entry = tk.Entry(config_frame, show="*", width=50)
    sftp_pass_entry.grid(row=6, column=1, padx=10, pady=5)

    tk.Label(config_frame, text="Cron Expression:").grid(row=7, column=0, padx=10, pady=5)
    cron_entry = tk.Entry(config_frame, width=50)
    cron_entry.grid(row=7, column=1, padx=10, pady=5)

    tk.Button(config_frame, text="Schedule Task", command=start_job).grid(row=8, column=0, columnspan=2, pady=10)

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
