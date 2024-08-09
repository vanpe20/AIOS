
from pyopenagi.agents.base_agent import BaseAgent

import time

from pyopenagi.agents.agent_process import (
    AgentProcess
)

from pyopenagi.utils.chat_template import Query
from aios.storage.db_sdk import Data_Op
import threading
import argparse

from concurrent.futures import as_completed
import re
import json
import os
import logging
import subprocess
import redis
from pyopenagi.utils.filereader import update_file

import numpy as np

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='/Users/manchester/Documents/rag/AIOS/change_record.log')
class RollbackAgent(BaseAgent):
    def __init__(self,
                 agent_name,
                 task_input,
                 agent_process_factory,
                 log_mode,
                 use_llm = None,
                 data_path = None,
                 raw_datapath = None,
                 monitor_path = None,
                 sub_name=None,
        ):
        BaseAgent.__init__(self, agent_name, task_input, agent_process_factory, log_mode)
        self.data_path = data_path
        self.raw_datapath = raw_datapath
        self.use_llm = use_llm
        self.sub_name = sub_name
        self.monitor_path = monitor_path
        self.file_mod_times = {}
        self.active = False
        retric_dic = {}
        self.tools = None
        pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
        self.redis_client = redis.Redis(connection_pool=pool)
        self.database = Data_Op(retric_dic,self.redis_client)

    def build_system_instruction(self):
        prefix = "".join(
            [
                "".join(self.config["description"])
            ]
        )        
        self.messages.append(
                {"role": "system", "content": prefix }
            )

    
    def automatic_workflow(self):
        return super().automatic_workflow()
    
    def manual_workflow(self):
        pass
    
    def stop_monitoring(self):
        self.active = False
    
    def match(self,text):
        name_match = re.search(r"Name:\s*(\w+)", text)
        name = name_match.group(1) if name_match else None

        # 提取日期
        date_match = re.search(r"Date:\s*([\d-]+)", text)
        date = date_match.group(1) if date_match else None
        
        return name, date
    def rollback(self,name,date):
        key_with_name = []
        cursor = '0'
        while cursor != 0:
            cursor, keys = self.redis_client.scan(cursor=cursor, match='*{}*'.format(name), count=100)
            key_with_name.extend([key.decode('utf-8') for key in keys])
        if len(key_with_name) > 1:
            print(f"\nThere is more than one term containing {name}, the list of them is {key_with_name}")
            db_name = input("You should choose one of them:")
        elif len(key_with_name) == 0:
            raise Exception("No such file")
        else:
            db_name = key_with_name[0]

        ll = self.redis_client.llen(db_name)
        files = None
        for i in range(ll):
            file_info_json = self.redis_client.lindex(db_name, i)
            file_info = json.loads(file_info_json)
            if file_info['last_modified_date'] == date:
                files  = file_info
        if files is None:
            raise ValueError(f"\nThe file didn't change in {date} or it has been overdued")
        
        document = files['content']
        db_path = files['db_path']
        sub_name = files['sub_name']
        text_path = files['text_path']

        update_file(text_path,document)

        collection_fore = self.database.get_collection(db_path,sub_name,metaname=name)
        text_before = collection_fore.get()['documents']
        text_date = collection_fore.get()['metadatas'][0]['last_modified_date']
        self.version(name,text_date,text_before,text_path,db_path,sub_name)
        self.database.update(db_path,sub_name,document, obj=name)
 
    def run(self):
        self.redis_client.select(1)
        self.build_system_instruction()

        task_input = "The task you need to solve is: " + self.task_input

        self.logger.log(f"{task_input}\n", level="info")


        request_waiting_times = []
        request_turnaround_times = []
        rounds = 0
        workflow = self.config['workflow']
        for i, step in enumerate(workflow):
            prompt = f"\nAt current step, you need to {workflow} {self.task_input}"
            self.messages.append(
                    {"role": "user", 
                    "content": prompt}
                )
            tool_use = None
            response, start_times, end_times, waiting_times, turnaround_times = self.get_response(
            query = Query(
                    messages = self.messages,
                    tools = tool_use
                    )
            )

            response_message = response.response_message
            request_waiting_times.extend(waiting_times)
            request_turnaround_times.extend(turnaround_times)

            if i == 0:
                self.set_start_time(start_times[0])

                # tool_calls = response.tool_calls
                
            self.messages.append({
                    "role": "user",
                    "content": response_message
                })

            if i == len(workflow) - 1:
                final_result = self.messages[-1]
            self.logger.log(f"{response_message}\n", level="info")
            rounds += 1
        self.set_status("done")
        self.set_end_time(time=time.time())
        
        name, date = self.match(response_message)
        self.rollback(name,date)

        return {
            "agent_name": self.agent_name,
            "result": final_result,
            "rounds": rounds,
            "agent_waiting_time": self.start_time - self.created_time,
            "agent_turnaround_time": self.end_time - self.created_time,
            "request_waiting_times": request_waiting_times,
            "request_turnaround_times": request_turnaround_times,
        }


    def version(self,text_name,texr_date,text_before,text_path,db_path):
        self.redis_client.select(1)

        file_info = {
            "file_name": text_name,
            "last_modified_date": texr_date,
            "content": text_before,
            "text_path": text_path,
            "db_path":db_path
        }
        redis_key = file_info['file_name']
        file_info_json = json.dumps(file_info)

        if self.redis_client.llen(redis_key) > 5 and self.redis_client.exists(redis_key):
            loc = self.redis_client.lindex(redis_key,-1)
            self.redis_client.ltrim(loc,0,9)

        self.redis_client.rpush(redis_key, file_info_json)

    def parse_result(self, prompt):
        return prompt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run NarrativeAgent')
    parser.add_argument("--agent_name")
    parser.add_argument("--task_input")

    "Please search  "
    # args = parser.parse_args()
    # agent = FileAgent(args.agent_name, args.task_input)
    # agent.run()
