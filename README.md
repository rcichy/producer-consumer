# producer-consumer

## **How to run:**

#### Set env vars for consumer:

`export REDIS_HOST=127.0.0.1`

`export REDIS_PORT=6379`

`export REDIS_CHANNEL=test`

`export DATABASE_URI=postgresql://...`

`export REDIS_PROCESSING_KEY=consumer1`

Every consumer should have different `REDIS_PROCESSING_KEY`. If a consumer fails, he will reprocess unfinished tasks using his processing list under the REDIS_PROCESSING_KEY key.

#### Run consumer:
`python3 consumer.py`
Can be run multiple times, with different `REDIS_PROCESSING_KEY`.

#### Set env var for producer:

`export REDIS_HOST=127.0.0.1`

`export REDIS_PORT=6379`

`export REDIS_CHANNEL=test`

#### Run producer
python3 producer.py ~/repos/data.csv
 
 
## **How it works:**
Producer will read the csv file and push its contents to the list under REDIS_TASKS_KEY key. He will also notify consumer via notification channel under REDIS_CHANNEL key.

Consumers will listing on the notification channel and will pick a task by moving it from the producer list to its own consumer list under REDIS_PROCESSING_KEY. Then he will process the task (add it to the database) and remove for the consumer list.

Consumer will read its own consumer tasks list on boot so it will retry tasks that failed.