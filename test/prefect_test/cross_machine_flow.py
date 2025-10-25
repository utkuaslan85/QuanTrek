from prefect import flow, task
from datetime import datetime
import socket
import time

@task
def oc_job1():
    """First job on Oracle Cloud"""
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ OC_JOB1 executed on {hostname} at {timestamp}")
    time.sleep(2)  # Simulate some work
    return f"oc_job1_result from {hostname}"

@task
def oc_job2(prev_result):
    """Second job on Oracle Cloud"""
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ OC_JOB2 executed on {hostname} at {timestamp}")
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return f"oc_job2_result from {hostname}"

@task
def wd_job1(prev_result):
    """First job on WSL Desktop"""
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ WD_JOB1 executed on {hostname} at {timestamp}")
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return f"wd_job1_result from {hostname}"

@task
def wd_job2(prev_result):
    """Second job on WSL Desktop"""
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ WD_JOB2 executed on {hostname} at {timestamp}")
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return f"wd_job2_result from {hostname}"

@task
def oc_job3(prev_result):
    """Final job on Oracle Cloud"""
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"✅ OC_JOB3 executed on {hostname} at {timestamp}")
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return f"oc_job3_completed from {hostname}"

@flow(name="cross-machine-workflow")
def cross_machine_flow():
    """
    Execute jobs in sequence across OC and WD:
    oc_job1 -> oc_job2 -> wd_job1 -> wd_job2 -> oc_job3
    """
    print("🚀 Starting cross-machine workflow...")
    
    # Execute on Oracle Cloud
    result1 = oc_job1.submit(task_run_name="oc_job1")
    result2 = oc_job2.submit(result1, task_run_name="oc_job2", wait_for=[result1])
    
    # Switch to WSL Desktop
    result3 = wd_job1.submit(result2, task_run_name="wd_job1", wait_for=[result2])
    result4 = wd_job2.submit(result3, task_run_name="wd_job2", wait_for=[result3])
    
    # Back to Oracle Cloud
    result5 = oc_job3.submit(result4, task_run_name="oc_job3", wait_for=[result4])
    
    final_result = result5.result()
    print(f"🎉 Workflow completed! Final result: {final_result}")
    return final_result

if __name__ == "__main__":
    cross_machine_flow()