import os
import json
import asyncio
import httpx
import secrets
from datetime import datetime


class Schedule:
    HEADER = {
        'token': os.getenv("TRACKCASH_TOKEN")
    }
    CONFIGS_URL = "URL REMOVIDO POR QUESTÕES DE SEGURANÇA E CONFIDENCIALIDADE"
    FIRST_PAGE = "URL REMOVIDO POR QUESTÕES DE SEGURANÇA E CONFIDENCIALIDADE"
    PROCESSES_URL = "URL REMOVIDO POR QUESTÕES DE SEGURANÇA E CONFIDENCIALIDADE"

    def __init__(self, event):
        self.event = event
        self.process = event['process']
        self.attempts = event.get('attempts', 0)
        self.page = event.get('next_page', Schedule.FIRST_PAGE)
        self.exist_process = False
        self.generated_configs = []
        self.error = False
        self.all_processes = httpx.get(Schedule.PROCESSES_URL).json()

    def run_process(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_schedules())

    async def get_schedules(self):
        try:
            schedule_url = self.page
            for process in self.all_processes:
                if process.get('process') == self.process and process.get('enable') is True:
                    self.exist_process = True

                    response = httpx.get(
                        schedule_url,
                        headers=Schedule.HEADER,
                        params={'id_process': process['process'], 'date': str(datetime.now().strftime('%Y-%m-%d'))},
                        follow_redirects=True
                    )
                    if response.status_code == 200:
                        self.page = response.json().get('next_page_url', None)
                    else:
                        self.page = None
                        return

                    schedules = [dic for dic in response.json()['data'] if dic["status"] in ["0", "2"]]

                    if len(schedules) == 0:
                        if self.page:
                            await self.get_schedules()
                        elif not self.page:
                            return

                    tasks = []
                    batch_size = process.get('concurrency')
                    for i in range(0, len(schedules), batch_size):
                        group = schedules[i:i + batch_size]
                        for scheduling in group:
                            tasks.append(Schedule.get_config(scheduling, process))
                        configs = await asyncio.gather(*tasks)
                        self.generated_configs.extend(configs)
                        tasks = []

        except Exception as err:
            self.error = err

    @staticmethod
    async def get_config(scheduling, process):
        async with httpx.AsyncClient() as client:
            config = {}
            body = {
                'id': scheduling['id'],
                'id_store': int(scheduling['id_store']),
                'account': int(scheduling['id_account']),
                'start': json.loads(scheduling['cron'])['startdate'],
                'end': json.loads(scheduling['cron'])['enddate']
            }

            params = {
                'id_store': scheduling['id_store'],
                'account': scheduling['id_account'],
                'code': process['channel'],
                'key': 'access,account,general_config,status,startdate'
            }

            response_configs = await client.get(Schedule.CONFIGS_URL, headers=Schedule.HEADER, params=params)
            response_configs.raise_for_status()
            response_configs = response_configs.json()
            auth = {}
            for data in response_configs['data']:
                if 'startdate' in data['key']:
                    auth[data['key']] = data['value']
                    continue
                auth.update(data.get('value'))
            body['auth'] = auth
            body['job'] = scheduling['id_process']
            body['cron'] = scheduling['cron']
            body['attempts'] = 0
            body["bucket"] = "trackcash-file-channel-input"
            config['event'] = body if body is not None else []
            config['channel'] = process
            config['event']['executionName'] = f"id.{config['event']['id']}_" \
                                               f"store.{scheduling['id_store']}_" \
                                               f"acc.{scheduling['id_account']}_" \
                                               f"start.{config['event']['start']}_" \
                                               f"end.{config['event']['end']}_" \
                                               f"{secrets.token_urlsafe(4)}"
            return config

    def handle_response(self):
        self.event["process"] = self.process
        self.event["job"] = 610
        self.event["code"] = 100 if not self.error else 400
        self.event["code"] = 200 if not self.generated_configs and not self.error else self.event["code"]
        self.event["code"] = 400 if not self.exist_process else self.event['code']
        self.event['next_page'] = self.page if self.page != Schedule.FIRST_PAGE else None
        self.event['body'] = self.generated_configs
        self.event['attempts'] = self.event.get('attempts', 0) + 1 \
            if self.event['code'] == 400 else self.event.get('attempts', 0)
