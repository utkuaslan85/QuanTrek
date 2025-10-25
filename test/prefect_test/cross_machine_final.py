from prefect import flow
import socket
import time
from datetime import datetime

# Create separate flow files that will be deployed independently

# ============ OC JOBS ============
@flow(name="oc-job-1", log_prints=True)
def oc_job1():
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB1 on {hostname} at {timestamp}"
    print(msg)
    time.sleep(2)
    return msg

@flow(name="oc-job-2", log_prints=True)
def oc_job2(prev_result: str):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB2 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return msg

@flow(name="oc-job-3", log_prints=True)
def oc_job3(prev_result: str):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ OC_JOB3 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return msg

# ============ WD JOBS ============
@flow(name="wd-job-1", log_prints=True)
def wd_job1(prev_result: str):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ WD_JOB1 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return msg

@flow(name="wd-job-2", log_prints=True)
def wd_job2(prev_result: str):
    hostname = socket.gethostname()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ WD_JOB2 on {hostname} at {timestamp}"
    print(msg)
    print(f"   Received: {prev_result}")
    time.sleep(2)
    return msg

# ============ ORCHESTRATOR ============
@flow(name="orchestrator", log_prints=True)
async def orchestrator():
    """Main orchestrator that triggers flows on different machines"""
    from prefect.deployments import run_deployment
    
    print("\n🚀 Starting cross-machine workflow...\n")
    
    # Run deployments in sequence
    print("▶️  Running OC_JOB1...")
    r1 = await run_deployment(
        name="oc-job-1/oc-job-1",
        timeout=0,  # Wait for completion
    )
    
    print("▶️  Running OC_JOB2...")
    r2 = await run_deployment(
        name="oc-job-2/oc-job-2",
        parameters={"prev_result": str(r1)},
        timeout=0,
    )
    
    print("▶️  Running WD_JOB1...")
    r3 = await run_deployment(
        name="wd-job-1/wd-job-1",
        parameters={"prev_result": str(r2)},
        timeout=0,
    )
    
    print("▶️  Running WD_JOB2...")
    r4 = await run_deployment(
        name="wd-job-2/wd-job-2",
        parameters={"prev_result": str(r3)},
        timeout=0,
    )
    
    print("▶️  Running OC_JOB3...")
    r5 = await run_deployment(
        name="oc-job-3/oc-job-3",
        parameters={"prev_result": str(r4)},
        timeout=0,
    )
    
    print("\n🎉 Workflow completed!\n")
    return r5

if __name__ == "__main__":
    # Deploy all flows
    print("🚀 Deploying flows...\n")
    
    oc_job1.deploy(name="oc-job-1", work_pool_name="oc-pool", cwd="/mnt/vol1/prefect_test")
    oc_job2.deploy(name="oc-job-2", work_pool_name="oc-pool", cwd="/mnt/vol1/prefect_test")
    oc_job3.deploy(name="oc-job-3", work_pool_name="oc-pool", cwd="/mnt/vol1/prefect_test")
    
    wd_job1.deploy(name="wd-job-1", work_pool_name="wd-pool", cwd="/mnt/vol1/prefect_test")
    wd_job2.deploy(name="wd-job-2", work_pool_name="wd-pool", cwd="/mnt/vol1/prefect_test")
    
    orchestrator.deploy(name="orchestrator", work_pool_name="oc-pool", cwd="/mnt/vol1/prefect_test")
    
    print("\n✅ All deployments created!")
    print("\nNext steps:")
    print("1. On OC: prefect worker start --pool oc-pool")
    print("2. On WD: prefect worker start --pool wd-pool")
    print("3. Run: prefect deployment run 'orchestrator/orchestrator'")