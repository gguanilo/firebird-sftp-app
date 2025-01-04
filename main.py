import logging
import os
from dotenv import load_dotenv
from firebird.FirebirdHandler import FirebirdHandler
from sftp.SFTPHandler import SFTPHandler
from utils.Logger import Logger
from utils.errors import FirebirdConnectionError, FirebirdQueryError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
import re

Logger.setup_logging()
# Load environment variables
load_dotenv()

# Firebird configuration
firebird_config = {
    "host": os.getenv("FIREBIRD_HOST"),
    "port": int(os.getenv("FIREBIRD_PORT")),
    "database": os.getenv("FIREBIRD_DATABASE"),
    "user": os.getenv("FIREBIRD_USER"),
    "password": os.getenv("FIREBIRD_PASSWORD")
}

# Initialize scheduler
scheduler = BackgroundScheduler()

def save_task_to_db(task_details):
    try:
        db_handler = FirebirdHandler(**firebird_config)
        db_handler.connect()
        db_handler.insert_task(
            task_name=task_details["name"],
            query=task_details["query"],
            output_file=task_details["output_file"],
            remote_path=task_details["remote_path"],
            sftp_host=task_details["sftp_host"],
            sftp_user=task_details["sftp_user"],
            cron_expression=task_details["cron_expression"]
        )
        db_handler.close()
    except Exception as e:
        logging.error(f"Error saving task to database: {e}")
        raise

def fetch_tasks_from_db():
    try:
        db_handler = FirebirdHandler(**firebird_config)
        db_handler.connect()
        tasks = db_handler.get_tasks()
        db_handler.close()
        return tasks
    except Exception as e:
        logging.error(f"Error fetching tasks from database: {e}")
        return []

def job(task_name, query, output_file, remote_path, sftp_host, sftp_user, sftp_pass):
    """
    Job to run the process of fetching data, saving to a file, and uploading it.
    """
    sftp_config = {
        "host": sftp_host,
        "username": sftp_user,
        "password": sftp_pass,
        "port": int(os.getenv("SFTP_PORT", 2222))
    }

    db_handler = FirebirdHandler(**firebird_config)
    sftp_handler = SFTPHandler(**sftp_config)

    try:
        db_handler.connect()
        db_handler.execute_query_to_csv(query, output_file)
        sftp_handler.connect()
        sftp_handler.upload_file(output_file, remote_path)
        logging.info(f"Task {task_name} executed successfully.")
    except Exception as e:
        logging.error(f"Error executing task {task_name}: {e}")
    finally:
        db_handler.close()
        sftp_handler.close_connection()

def schedule_task(task):
    """
    Schedule a task based on its cron expression.
    """
    logging.info(f"Attempting to schedule task: {task}")
    cron_expression = task.get("CRON_EXPRESSION")
    if cron_expression:
        try:
            logging.info(f"Parsing cron expression: {cron_expression}")
            cron_parts = cron_expression.split()
            logging.info(f"Cron parts: {cron_parts}")

            scheduler.add_job(
                job,
                trigger=CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4]
                ),
                args=[
                    task["TASK_NAME"],
                    task["QUERY"],
                    task["OUTPUT_FILE"],
                    task["REMOTE_PATH"],
                    task["SFTP_HOST"],
                    task["SFTP_USER"],
                    "password"  # Replace with actual password management
                ],
                id=str(task["ID"]),
                name=task["TASK_NAME"],
                replace_existing=True
            )
            logging.info(f"Task {task['TASK_NAME']} scheduled successfully.")
        except Exception as e:
            logging.error(f"Error scheduling task {task['TASK_NAME']}: {e}")
    else:
        logging.info("No cron expression provided; skipping task scheduling.")


def load_and_schedule_tasks():
    """
    Load tasks from the database and schedule them.
    """
    logging.info("Fetching tasks from the database to schedule them.")
    tasks = fetch_tasks_from_db()
    logging.info(f"Fetched {len(tasks)} tasks from the database.")
    for task in tasks:
        logging.info(f"Scheduling task: {task['TASK_NAME']} (ID: {task['ID']})")
        schedule_task(task)
    logging.info("All tasks have been scheduled.")


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
            task_list.insert("", "end", values=(task.get("ID"), task.get("TASK_NAME"), task.get("STATUS")))

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
    Logger.setup_logging()
    load_and_schedule_tasks()
    scheduler.start()
    open_gui()
