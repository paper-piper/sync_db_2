import win32file
import win32process
import win32con
import win32event
import logging
import os
from logging_utils import setup_logger
# Configuration
NUM_READERS = 5
NUM_WRITERS = 5
DB_FILE = "database.db"
LOOP_TIMES = 10

logger = setup_logger("Windows_processes")


def start_process(role, filename, key, value=None):
    command = f'python worker_script.py {role} {filename} {key} {value or 0} {LOOP_TIMES}'
    process_info = win32process.CreateProcess(
        None,               # Application name (use None if specifying the command line)
        command,            # Command line
        None,               # Process security attributes
        None,               # Thread security attributes
        False,              # Inherit handles
        win32process.CREATE_NO_WINDOW,  # Creation flags
        None,               # Environment
        os.getcwd(),        # Current directory
        win32process.STARTUPINFO()
    )
    logger.info(f"Started {role} process with key={key} and value={value}")
    return process_info[0]  # Return the process handle


def main():
    logger.info("Starting the SynchronizerDB process simulation.")

    # Start reader processes
    readers = [start_process("reader", DB_FILE, i) for i in range(NUM_READERS)]
    logger.info(f"Started {NUM_READERS} reader processes.")

    # Start writer processes
    writers = [start_process("writer", DB_FILE, 0, 5) for _ in range(NUM_WRITERS)]
    logger.info(f"Started {NUM_WRITERS} writer processes.")

    # Wait for all processes to complete
    for handle in readers + writers:
        win32event.WaitForSingleObject(handle, -1)
        win32file.CloseHandle(handle)  # CloseHandle from win32process
        logger.info("Process completed and handle closed.")

    logger.info("All processes completed. Finalizing run.")

    print("All processes completed.")


if __name__ == "__main__":
    main()
