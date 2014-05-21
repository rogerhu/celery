if __name__ == "__main__":
    # 1. Clear all queues.
    # 2. Startup worker:
    # CSTRESS_BACKEND="redis://localhost:6379/2" CSTRESS_BROKER="redis://localhost:6379/1" python go.py worker --concurrency=8 --maxtasksperchild=2 --loglevel=info
    # 3. Startup task:
    # celery/funtests/stress$ CSTRESS_BACKEND="redis://localhost:6379/2" CSTRESS_BROKER="redis://localhost:6379/1" /home/rhu/.virtualenvs/dev/bin/python -m stress --group green
    import stress.app
    stress.app.start()

