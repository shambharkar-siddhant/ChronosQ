ChronosQ — A File-Based Distributed Job Queue for Low-Infra Environments
============================================================================

### **A lightweight, fault-tolerant job dispatcher built using Python + filesystem primitives.**

Designed for environments where Kafka/RabbitMQ/Celery are too heavy, but reliability, recoverability and auditability still matter.

🔧 Overview
-----------

**ChronosQ** is a modular, event-driven **job orchestration system** that turns incoming _events_ into _jobs_, assigns them to _workers_, tracks their lifecycle via atomic filesystem states, and automatically **recovers** from crashes or partial processing.

It’s ideal for:

*   small teams without queueing infrastructure
    
*   automation pipelines
    
*   human + bot hybrid workflows
    
*   data processing workflows
    
*   offline or sandboxed environments
    

> Think of it as:  **“A poor-man’s Kafka + Celery — implemented with files, but architected like a distributed system.”**

🚀 Key Features
---------------

*   **Event → Job Transformation:** Automatically converts JSON event files into job units.
    
*   **Deterministic Job Assignment:** Round-robin or rule-based worker selection for fair distribution.
    
*   **Worker Directories as Queues:**

	`broker/types/<task_type>/<worker_id>/<job_id>.json`
    

	```python
    pending → assigned → processing → success / failure / hold

*   **Crash-Safe Recovery Engine:** Detects stuck .processing jobs and requeues or fails gracefully.
    
*   **At-Least-Once Delivery Guarantee:** Job retries with configurable attempt limit.
    
*   **Structured JSON Logging:** Every event is machine-parsable and audit-friendly.
    
*   **Zero External Dependencies:** Pure Python + filesystem. Runs anywhere.

*   **Crash-Safe Recovery Engine:** Detects stuck .processing jobs and requeues or fails gracefully.
    
*   **At-Least-Once Delivery Guarantee:** Job retries with configurable attempt limit.
    
*   **Structured JSON Logging:** Every event is machine-parsable and audit-friendly.
    
*   **Zero External Dependencies:** Pure Python + filesystem. Runs anywhere.

🏗 Architecture
----------------

 ### System Flow Diagram

                ┌────────────┐
                │  input/    │
                │  events    │
                └─────┬──────┘
                      │
                      ▼
            ┌───────────────────┐
            │ Event Processor   │
            │  (event→job)      │
            └──────┬────────────┘
                   │ job.json
                   ▼
            ┌───────────────────┐
            │ Pending Job Queue │
            │     jobs/         │
            └──────┬────────────┘
                   │ assign
                   ▼
        ┌──────────────────────────────┐
        │  Job Broker (Core Scheduler) │
        └──────┬───────────┬───────────┘
               │           │
       assigns ▼           ▼ assigns
    ┌────────────────┐ ┌────────────────┐
    │ worker_1 dir   │ │ worker_2 dir   │
    │ broker/.../u1  │ │ broker/.../u2  │
    └─────┬──────────┘ └─────┬─────────┘
          │ process          │ process
          ▼                  ▼
      success/failure     success/failure
          │                  │
          ▼                  ▼
         ┌───────────────────┐
         │ output/ or error/ │
         └───────────────────┘


🧩 Job Lifecycle
-----------------

ChronosQ models job state using filesystem transitions:

    pending (jobs/job_id.json)
        ↓ assign
    broker/types/<type>/<user>/job_id.json
        ↓ worker picks
    job_id.json.processing
        ↓ complete
    job_id.json.success  or  job_id.json.failure

**Each job has an associated .state.json metadata file tracking:**

* assignment history

* attempts

* timestamps

* worker id

* last error message

⚙️ Core Components
---------------------

### EventProcessor
* Watches `input/`

* Reads new event JSON

* Creates standardized job files

### ChronosQ
* Reads pending jobs

* Chooses eligible worker

* Moves job into the worker queue

* Creates `.state` metadata

* Writes structured logs

### RecoveryEngine

* Scans `.processing` jobs older than RECOVERY_TIMEOUT

* Requeues or marks as failure

* Ensures at-least-once delivery


📁 Directory Structure
-------------------------
    job-broker/
    broker/
        cli.py
        core.py
        config.py
        recovery.py
        fs_utils.py
        logging_utils.py
        models.py

    examples/
        dummy_worker.py
        sample_config.yaml
        sample_events/

    README.md
    pyproject.toml

🛠 Quick Start
------------------

### 1. Install
Clone Git repo

### 2. Initialize project structure
    broker init

### 3. Start the broker
    broker run --config sample_config.yaml

### 4. Add events
    input/event_123.json

### 5. Run the dummy worker
    python examples/dummy_worker.py --worker-id user_1

You'll see logs like:

    {
     "ts": "2025-01-01T10:30:21",
     "event": "job_assigned",
     "job_id": "job_abc",
     "worker_id": "user_1"
    }

### Sample Job

    {
     "job_id": "job_abc",
     "type": "quality_check",
     "payload": {
        "customer_id": 123,
        "doc_url": "https://example.com/file.pdf"
     },
     "created_at": "2025-01-01T10:20:00",
     "priority": "normal"
    }


⭐ If you find this useful, don't forget to star the repo!
--------------------------------