from prefect import flow, task, get_run_logger
from datetime import datetime
import socket
import time

# Tasks with specific work pool assignments
@task
def oc_job1():
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB1 on {hostname} at {timestamp}"
    print(msg)
    time.sleep(1)
    return msg

@task
def oc_job2(prev_result):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB2 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(1)
    return msg

@task
def wd_job1(prev_result):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ WD_JOB1 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(1)
    return msg

@task
def wd_job2(prev_result):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ WD_JOB2 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(1)
    return msg

@task
def oc_job3(prev_result):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB3 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(1)
    return msg

@flow(name="cross-machine-workflow", log_prints=True)
def cross_machine_flow():
    """Execute jobs in sequence: oc_job1 -> oc_job2 -> wd_job1 -> wd_job2 -> oc_job3"""
    
    print("\n🚀 Starting cross-machine workflow...\n")
    
    # Execute sequentially
    r1 = oc_job1()
    r2 = oc_job2(r1)
    r3 = wd_job1(r2)
    r4 = wd_job2(r3)
    r5 = oc_job3(r4)
    
    print("\n🎉 Workflow completed!\n")
    return r5

if __name__ == "__main__":
    cross_machine_flow()