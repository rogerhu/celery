from celery import current_app
send_task = current_app.send_task
