from schedule import Schedule
import time
import json


def run(event, context=None):
    try:
        if event.get('body'):
            del event['body']
        instance = Schedule(event)
        instance.run_process()
        instance.handle_response()
        event = instance.event
    except Exception as err:
        event['code'] = 400
        event['attempts'] = event.get('attempts', 0) + 1
        event['message'] = f"{err}"

    return event


if __name__ == "__main__":
    start = time.time()
    print(run({"process": 2000}))
    end = time.time()
    print(end - start)
