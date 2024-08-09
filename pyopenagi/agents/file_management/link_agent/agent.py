from ...LinkAgent import LinkAgent

class LinkGenerateAgent(LinkAgent):
    def __init__(self,
                 agent_name,
                 task_input,
                 agent_process_factory,
                 log_mode:str,
                 use_llm = None,
                 data_path = None,
                 sub_name=None,
                 raw_datapath = None,
                 monitor_path = None
        ):
        LinkAgent.__init__(self,agent_name,task_input,agent_process_factory,log_mode, data_path, use_llm, raw_datapath=raw_datapath,monitor_path=monitor_path)
        self.workflow_mode = "automatic"

        def manaul_workflow(self):
            pass
        
        def run(self):
            return super().run()