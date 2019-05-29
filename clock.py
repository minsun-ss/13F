from apscheduler.schedulers.blocking import BlockingScheduler
from rq import Queue
from worker import conn
import app
import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sched = BlockingScheduler(timezone=utc)
q = Queue(connection=conn)

# schedule a job monday to friday, at 5 PM
@sched.scheduled_job('cron', day= 1-5, hour=21)
def scheduled_job():
    q.enqueue(app.everyday())
    print('This job is run every weekday day at around 5 PM.')
    sys.stdout.flush()

sched.start()
