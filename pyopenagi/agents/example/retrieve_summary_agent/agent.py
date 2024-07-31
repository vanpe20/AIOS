from ...FileAgent import FileAgent

class RetrieveSummaryAgent(FileAgent):
    def __init__(self,
                 agent_name,
                 task_input,
                 data_path,
                 use_llm,
                 agent_process_factory,
                 log_mode:str,
                 sub_name=None,
                 raw_datapath = None,
        ):
        FileAgent.__init__(self,agent_name,task_input,data_path,use_llm,agent_process_factory,log_mode)
        self.workflow_mode = "automatic"

        def manaul_workflow(self):
            pass
        
        def run(self):
            return super().run()