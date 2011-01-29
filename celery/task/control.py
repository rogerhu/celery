from celery import current_app

broadcast = current_app.control.broadcast
rate_limit = current_app.control.rate_limit
ping = current_app.control.ping
revoke = current_app.control.revoke
discard_all = current_app.control.discard_all
inspect = current_app.control.inspect
