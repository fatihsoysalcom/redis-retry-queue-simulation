# Redis Retry Queue Simulation

This Python script simulates a Redis-based retry queue for handling failed subscription renewals. It demonstrates how to enqueue tasks, process them, and implement a delayed retry mechanism using Redis sorted sets.

## Language

`python`

## How to Run

1. Ensure Redis is running on localhost:6379.
2. Run the script: `python redis_retry_queue.py`
3. Press Ctrl+C to stop.

## Original Article

This example accompanies the Turkish article: [Redis Retry Queues vs AWS SQS: Başarısız Yayın Yenilemelerinde Hangi Çözüm Daha İyi?](https://fatihsoysal.com/blog/redis-retry-queues-vs-aws-sqs-basarisiz-yayin-yenilemelerinde-hangi-cozum-daha-iyi/).

## License

MIT — see [LICENSE](LICENSE).
