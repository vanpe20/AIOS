
from pyopenagi.agents.base_agent import BaseAgent

import time

from pyopenagi.agents.agent_process import (
    AgentProcess
)

from pyopenagi.utils.chat_template import Query
from aios.storage.db_sdk import Data_Op

import argparse

from concurrent.futures import as_completed
import re
import json
import os
import logging
import subprocess

import numpy as np
class FileAgent(BaseAgent):
    def __init__(self,
                 agent_name,
                 task_input,
                 data_path,
                 use_llm,
                 agent_process_factory,
                 log_mode,
                 sub_name=None,
                 raw_datapath = None,
        ):
        BaseAgent.__init__(self, agent_name, task_input, agent_process_factory, log_mode)
        self.data_path = data_path
        self.raw_datapath = raw_datapath
        self.use_llm = use_llm
        self.sub_name = sub_name
        retric_dic = {}
        self.database = Data_Op(retric_dic)
        # self.tool_list = {
        #     "database_method": Data_Op()
        # }
        # self.workflow = self.config["workflow"]
        self.tools = None

    def match(self,input,mode):
        if mode == 'add':
            pattern = r"Please add (\w+) to (\w+) of database (\w+)"
            match = re.search(pattern, input)
            if match:
                alter_data, metaname, sub_name = match.groups(1),match.groups(2),match.groups(3)
            else:
                raise ValueError('Please input order in correct format')
            return alter_data, metaname, sub_name
        elif mode == 'delete':
            pattern1 = r"Please delete (\w+) of database (\w+)"
            pattern2 = r"Please delete the content related to (.+?) from (\w+)"
            match1 = re.search(pattern1, input)
            match2 = re.search(pattern2, input)
            if match1:
                metaname, sub_name = match1.groups(1),match1.groups(2)
                return metaname, sub_name, 'delete_collection'
            elif match2:
                text, sub_name = match2.groups(1),match2.groups(2)
                return text, sub_name, 'delete_correlation'
            else:
                raise ValueError('Please input order in correct format')
        elif mode == 'alter':
            pattern = r"Please change (\w+) to (\w+) of database (\w+)"
            match = re.search(pattern, input)
            if match:
                alter_data, metaname, sub_name = match.groups(1),match.groups(2),match.groups(3)
            else:
                raise ValueError('Please input order in correct format')
            return alter_data, metaname, sub_name
        elif mode == 'retrieve':
            pattern = r"Please retrieve the content about (.+?) in (\w+) of database (\w+) by (\w+)"
            match = re.search(pattern, input)
            if match:
                query, metaname, sub_name, method =  match.groups(1),match.groups(2),match.groups(3),match.groups(4)
                return query, metaname, sub_name, method
            else:
               raise ValueError('Please input order in correct format')
        elif mode == 'join':
            pattern = r"Please join (\w+) of database (\w+) to (\w+) of database (\w+)"
            match = re.search(pattern,input)
            if match:
                metaname2,sub_name2,metaname1,subname1 = match.groups(1),match.groups(2),match.groups(3),match.groups(4)
                return  metaname2,sub_name2,metaname1,subname1
            else:
                raise ValueError('Please input order in correct format')
        elif mode == 'contains':
            pattern = r"Please search paper contains (.+?) from (\w+)"
            match = re.search(pattern,input)
            if match:
                query = match.group(1)
                sub_name = match.group(2)
                return query,sub_name
            else:
                raise ValueError('Please input order in correct format')
        elif mode == 'about':
            pattern = r"Please search for top (\w+) papers about (.+?) from (\w+) "
            match = re.match(pattern,input)
            if match:
                top, query, sub_name = match.groups(1),match.groups(2),match.groups(3)
                return int(top), query, sub_name
            else:
                raise ValueError('Please input order in correct format')

    def pre_rag(self):
        if not os.path.exists(self.data_path):
            if self.raw_datapath is None:
                raise ValueError('Database is not existing must create it but no raw data')
            else:
                self.database.create(self.data_path,self.sub_name,self.raw_datapath)
        else:
            if re.match(r"Please add", self.task_input):
                alter_data, metaname, sub_name = self.match(self.task_input,'add')
                self.database.insert(self.data_path,sub_name,alter_data,metaname)
            elif re.match(r"Please delete", self.task_input):
                metaname, sub_name, type = self.match(self.task_input,'delete')
                if type == 'delete_collection':
                    self.database.delete(self.data_path,sub_name,metaname=metaname)
                else:
                     self.database.delete(self.data_path,sub_name,text=metaname)
            elif re.match(r"Please alter", self.task_input):
                alter_data, metaname, sub_name = self.match(self.task_input,'alter')
                self.database.update(self.data_path,sub_name,alter_data,metaname)
            elif re.match(r"Please retrieve", self.task_input):
                query, metaname, sub_name, method = self.match(self.task_input,'retrieve')
                self.database.retrieve(self.data_path,sub_name,query,method,metaname)
            elif re.match(r"Please join", self.task_input):
                metaname2,sub_name2,metaname1,sub_name1 = self.match(self.task_input,'join')
                self.database.join(self.data_path,sub_name1,metaname1,metaname2,sub_name2)
            elif re.match(r"Please search paper contains",self.task_input):
                query,sub_name = self.match(self.task_input,'contains')
                if 'and' in query:
                    que = []
                    slices = query.split("and")
                    for i, s in enumerate(slices):
                        que.append(s)
                    ans, name = self.database.from_some_key_full(self.data_path,que,sub_name,con='and')
                elif 'or' in query:
                    que = []
                    slices = query.split("or")
                    for i, s in enumerate(slices):
                        que.append(s)
                    ans, name = self.database.from_some_key_full(self.data_path,que,sub_name,con='or')
                else:
                    ans, name = self.database.from_some_key_full(self.data_path,query,sub_name)
                return ans,name
            elif re.match(r"Please search paper about",self.task_input):
                top, query,sub_name = self.match(self.task_input,'contains')
                ans, name = self.database.from_some_key_sy(self.data_path,que,top,sub_name)
                return ans,name
            else:
                raise ValueError('Input Format Error!')
        
        return None,None

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

                
    def run(self):
        self.build_system_instruction()

        result = subprocess.run(['brew', 'services', 'start', 'redis'], check=True)
        result = subprocess.run(['brew', 'services', 'start', 'elastic/tap/elasticsearch-full'], check=True)
        
        if self.use_llm is False:
            task_input = "The task you need to solve is: " + self.task_input
        else:
            task_input = "The task you need to solve is: " + self.task_input + ' than summarize it'

        self.logger.log(f"{task_input}\n", level="info")

        ans, name = self.pre_rag()

        if ans is None and name is None:
            self.logger.log(f"{task_input} has been finished\n", level="info")
            return 

        if self.use_llm is False:
            self.logger.log(f"{name} include relevant content, the content is {ans} \n", level="info")
            return {
                "paper name":name,
                "result": ans
            }

        request_waiting_times = []
        request_turnaround_times = []

        rounds = 0
        result = []
        # response_message = ''
        workflow = self.config['workflow']
        for i, step in enumerate(workflow):
            for j in range(len(ans)):
                text = ans[j]
                keywords = 'References'
                if keywords in text:
                    parts = text.split(keywords)
                    text = parts[0]
                
                prompt = f"\nAt current step, you need to {workflow},<context>{text}</context>"
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

        return {
            "agent_name": self.agent_name,
            "result": final_result,
            "rounds": rounds,
            "agent_waiting_time": self.start_time - self.created_time,
            "agent_turnaround_time": self.end_time - self.created_time,
            "request_waiting_times": request_waiting_times,
            "request_turnaround_times": request_turnaround_times,
        }

    def parse_result(self, prompt):
        return prompt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run NarrativeAgent')
    parser.add_argument("--agent_name")
    parser.add_argument("--task_input")

    "Please search  "
    args = parser.parse_args()
    agent = FileAgent(args.agent_name, args.task_input)
    agent.run()
