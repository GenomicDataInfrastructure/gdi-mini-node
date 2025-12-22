import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

import mini_node.api
import mini_node.beacon
import mini_node.fdp
from mini_node.data import monitor_files
from mini_node.setup import app_version


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Data directory and S3 storage monitoring takes place
    # in a background thread:
    stop_event = threading.Event()
    data_monitor_thread = threading.Thread(
        name="data-monitor", target=monitor_files, args=(stop_event,),
    )
    data_monitor_thread.start()
    yield
    stop_event.set()
    data_monitor_thread.join()


app = FastAPI(title="GDI Mini Node", version=app_version, lifespan=lifespan)
mini_node.api.add_endpoints(app)
mini_node.fdp.add_endpoints(app)
mini_node.beacon.add_endpoints(app)
