# This is a main script that tests the functionality of specific agents.
# It requires no user input.


from aios_base.scheduler.fifo_scheduler import FIFOScheduler

import subprocess
from aios_base.utils.utils import (
    parse_global_args,
)

from pyopenagi.agents.agent_factory import AgentFactory

from pyopenagi.agents.agent_process import AgentProcessFactory

import warnings

from aios_base.llm_core import llms

from concurrent.futures import ThreadPoolExecutor, as_completed


from aios_base.utils.utils import delete_directories
from dotenv import load_dotenv
import redis
import logging

logging.basicConfig(filename='rs.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def clean_cache(root_directory):
    targets = {
        ".ipynb_checkpoints",
        "__pycache__",
        ".pytest_cache",
        "context_restoration",
    }
    delete_directories(root_directory, targets)


def main():
    # parse arguments and set configuration for this run accordingly
    warnings.filterwarnings("ignore")
    parser = parse_global_args()
    args = parser.parse_args()

    llm_name = args.llm_name
    max_gpu_memory = args.max_gpu_memory
    eval_device = args.eval_device
    max_new_tokens = args.max_new_tokens
    scheduler_log_mode = args.scheduler_log_mode
    agent_log_mode = args.agent_log_mode
    llm_kernel_log_mode = args.llm_kernel_log_mode
    use_backend = args.use_backend
    load_dotenv()

    llm = llms.LLM(
        llm_name=llm_name,
        max_gpu_memory=max_gpu_memory,
        eval_device=eval_device,
        max_new_tokens=max_new_tokens,
        log_mode=llm_kernel_log_mode,
        use_backend=use_backend
    )

    # run agents concurrently for maximum efficiency using a scheduler

    scheduler = FIFOScheduler(llm=llm, log_mode=scheduler_log_mode)

    agent_process_factory = AgentProcessFactory()

    agent_factory = AgentFactory(
        agent_process_queue=scheduler.agent_process_queue,
        agent_process_factory=agent_process_factory,
        agent_log_mode=agent_log_mode,
    )

    agent_thread_pool = ThreadPoolExecutor(max_workers=500)

    retric_dic = {}
    pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
    redis_client = redis.Redis(connection_pool=pool)
    result = subprocess.run(['brew', 'services', 'start', 'redis'], check=True)
    result = subprocess.run(['brew', 'services', 'start', 'elastic/tap/elasticsearch-full'], check=True)
        
    scheduler.start()

    # construct example agents


    # i = 0
    # time = 0
    # with open('/Users/manchester/Documents/rag/AIOS/test/files10_summary.txt', 'r') as file:
    #     for line in file:
    #         i +=1
    #         retrieve_summaryagent = agent_thread_pool.submit(
    #                         agent_factory.run_retrieve,
    #                         "file_management/retrieve_summary_agent",
    #                         line,
    #                         retric_dic,
    #                         redis_client,
    #                         '/Users/manchester/Documents/data/rag_test20',
    #                         True)
    #         agent_tasks = [retrieve_summaryagent]
    #         for r in as_completed(agent_tasks):
    #                     _res = r.result()
    #                     time += _res['agent_turnaround_time']
    #                     logging.info(_res['agent_turnaround_time'])

    # time = 0

    retrieve_summaryagent = agent_thread_pool.submit(
                agent_factory.run_retrieve,
                "file_management/retrieve_summary_agent",
                "Please search for the 2 papers with the highest correlation with large model uncertainty",
                # 'Please search for papers in llm_base, whose authors contain Mingyu Jin and Kai Mei.',
                retric_dic,
                redis_client,
                '/Users/manchester/Documents/data/rag_test1',
                True)
    agent_tasks = [retrieve_summaryagent]
    for r in as_completed(agent_tasks):
        _res = r.result()
    

    # change_monitoragent = agent_thread_pool.submit(
    #     agent_factory.run_retrieve,
    #    "file_management/change_monitor_agent",
    #     "Please change content in '/Users/manchester/Documents/rag/rag_source/physics/quantum.txt' to old_quan in physics",
    #     retric_dic,
    #     redis_client,
    #     '/Users/manchester/Documents/data/rag_database',
    #     True,
    #     '/Users/manchester/Documents/rag/rag_source/change_data/quantum.txt',
    #     '/Users/manchester/Documents/rag/rag_source/physics'
    # )
    # agent_tasks = [change_monitoragent]
    # for r in as_completed(agent_tasks):
    #     _res = r.result()



    # translation_agent = agent_thread_pool.submit(
    #     agent_factory.run_retrieve,
    #    "file_management/translation_agent",
    #     "Please translate file named quantum to Chinese",
    #     retric_dic,
    #     redis_client,
    #     '/Users/manchester/Documents/data/rag_database',
    #     True,
    # )
    # agent_tasks = [translation_agent]
    # for r in as_completed(agent_tasks):
    #         _res = r.result()

    # rollback_agent = agent_thread_pool.submit(
    #     agent_factory.run_retrieve,
    #    "file_management/rollback_agent",
    #     # "Please rollback file named quantum to the version in 2024-01-03",
    #     "Please rollback file named quantum 5 versions",
    #     retric_dic,
    #     redis_client,
    #     '/Users/manchester/Documents/data/rag_database',
    #     True,
    # )
    # agent_tasks = [rollback_agent]
    # for r in as_completed(agent_tasks):
    #     _res = r.result()


    # link_agent = agent_thread_pool.submit(
    #     agent_factory.run_retrieve,
    #    "file_management/link_agent",
    #     "/Users/manchester/Documents/rag/rag_source/rag_paper/AIOS.pdf ",
    #     retric_dic,
    #     redis_client,
    #     '/Users/manchester/Documents/data/rag_database',
    #     True,
    # )

    # agent_tasks = [link_agent]
    # for r in as_completed(agent_tasks):
    #         _res = r.result()









    scheduler.stop()

    clean_cache(root_directory="./")


if __name__ == "__main__":
    main()
