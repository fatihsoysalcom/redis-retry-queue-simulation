import redis
import time
import json
from datetime import datetime, timedelta

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
QUEUE_NAME = 'renewal_queue'
RETRY_QUEUE_PREFIX = 'retry_'
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 60 # Retry every minute for demonstration

# Initialize Redis connection
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def enqueue_renewal(user_id, subscription_id):
    """Adds a renewal task to the main queue."""
    task = {
        'user_id': user_id,
        'subscription_id': subscription_id,
        'attempt_count': 0,
        'enqueued_at': datetime.utcnow().isoformat()
    }
    r.lpush(QUEUE_NAME, json.dumps(task))
    print(f"[ENQUEUED] Renewal for user {user_id}, sub {subscription_id}")

def process_queue():
    """Processes tasks from the main queue and handles retries."""
    while True:
        # Try to get a task from the main queue
        task_data = r.brpop(QUEUE_NAME, timeout=1)[1] if r.brpop(QUEUE_NAME, timeout=1) else None

        if task_data:
            task = json.loads(task_data)
            user_id = task['user_id']
            subscription_id = task['subscription_id']
            attempt_count = task.get('attempt_count', 0)

            print(f"[PROCESSING] User: {user_id}, Sub: {subscription_id}, Attempt: {attempt_count + 1}")

            # Simulate renewal process
            success = simulate_renewal(user_id, subscription_id)

            if success:
                print(f"[SUCCESS] Renewal for user {user_id}, sub {subscription_id} completed.")
            else:
                print(f"[FAILED] Renewal for user {user_id}, sub {subscription_id} failed.")
                if attempt_count < MAX_RETRIES:
                    # Add to retry queue
                    task['attempt_count'] = attempt_count + 1
                    retry_key = f"{RETRY_QUEUE_PREFIX}{datetime.utcnow().timestamp() + RETRY_DELAY_SECONDS}"
                    r.zadd(QUEUE_NAME, {json.dumps(task): datetime.utcnow().timestamp() + RETRY_DELAY_SECONDS})
                    print(f"[RETRYING] Scheduled retry for user {user_id}, sub {subscription_id} in {RETRY_DELAY_SECONDS} seconds.")
                else:
                    print(f"[MAX RETRIES EXCEEDED] Renewal for user {user_id}, sub {subscription_id} permanently failed.")
        else:
            # Check for expired retry tasks
            now = datetime.utcnow().timestamp()
            # Find tasks whose score (retry timestamp) is <= now
            expired_tasks = r.zrangebyscore(QUEUE_NAME, '-inf', now)

            for task_data in expired_tasks:
                task = json.loads(task_data)
                # Remove from retry queue
                r.zrem(QUEUE_NAME, task_data)
                # Re-enqueue to the main queue for processing
                enqueue_renewal(task['user_id'], task['subscription_id'])
                print(f"[RE-ENQUEUED] Moving retry task for user {task['user_id']} back to main queue.")

        time.sleep(0.1) # Prevent busy-waiting

def simulate_renewal(user_id, subscription_id):
    """Simulates a renewal process that might fail."""
    # In a real scenario, this would involve calling an external API or payment gateway.
    # For demonstration, we'll randomly fail.
    import random
    return random.choice([True, True, False]) # 66% success rate

if __name__ == "__main__":
    print("Starting Redis Retry Queue simulation...")
    print(f"Queue: {QUEUE_NAME}")
    print(f"Retry Queue Prefix: {RETRY_QUEUE_PREFIX}")
    print(f"Max Retries: {MAX_RETRIES}")
    print(f"Retry Delay: {RETRY_DELAY_SECONDS} seconds")

    # Add some initial tasks
    enqueue_renewal('user1', 'sub_abc')
    enqueue_renewal('user2', 'sub_def')
    enqueue_renewal('user3', 'sub_ghi')

    # Start processing the queue
    try:
        process_queue()
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
