import network as NT

from typing import List, Optional
from enum import Enum
from pprint import pprint
from jsoncomment import JsonComment
import datetime



class GLOBAL_PARAMS:
    network_specs_path: str = "./network_specs.jsonc"
    jobs_specs_path: str = "./jobs_specs.json"

    simulation_step_s: int = 30
    print_state: bool = False 
    print_summary: bool = False
    # recursive_iteration: bool = True
    processing_speed: float = 1
    max_concurrent_jobs =  50




class ExecutionJob:
    class STATE(Enum):
        DEFINED = 1         # first definition of job
        ASSIGNED = 2        # data is being transferred to assigned node
        SENDING_IN = 3      # data transfers were scheduled     (sub-stage of ASSIGNED)
        ACTIVATED = 4       # all data is on location
        RUNNING = 5         # processing
        # TRANSFERING = 6     # possible transfer from local storage to dst
        FINISHED = 7        # output was produced   (holding)
        CLOSED = 8          # output data available at dst  (finished)

    def __init__(self, job_description: NT.JobDescription):
        self.state: ExecutionJob.STATE = ExecutionJob.STATE.DEFINED
        self.job_description: NT.JobDescription = job_description
        self.assigned_ProcessingNode: Optional[NT.ProcessingNode] = None
        self.assigned_NetworkNode: Optional[NT.NetworkNode] = None
        self.currently_using_Pnodes: Optional[NT.ProcessingNode] = None

        self.processing_speed_gbs: float = 0
        self.amount_processed_gb: float = 0

        self.D_longest_task = 0

    def start_processing(self):
        if self.assigned_ProcessingNode.ram_gb / self.assigned_ProcessingNode.cpu_cores < self.job_description.min_ram_per_core_gb:
            print("ERROR: Cannot execute job due to insufficient RAM.")
            exit(1)

        self.state = self.state.RUNNING
        self.reevaluate_processing_speed()

    def reevaluate_processing_speed(self):
        pnode_speed = 0
        for pNode in self.currently_using_Pnodes:
            pnode_speed += pNode.cpu_cores * pNode.cpu_speed_multiplier / self.job_description.difficulty_multiplier
            
        self.processing_speed_gbs = pnode_speed * GLOBAL_PARAMS.processing_speed * self.job_description.event_data_size_mb
        self.processing_speed_gbs /= (self.job_description.time_ms_per_event_per_core * 1e3)


    def process_data_tick(self, time_step_s: int):
        self.amount_processed_gb += self.processing_speed_gbs * time_step_s

    def __str__(self):
        ret = f"   Job name: {self.job_description.job_name}\n" \
              f"   Job state: {self.state}\n" \
              f"   assigned to: {self.assigned_ProcessingNode}\n"

        if self.state == ExecutionJob.STATE.RUNNING:
            ret += f"   percent done: {self.amount_processed_gb / self.job_description.total_src_data_size_gb()}"

        return ret



class ExecutionEnvironment:
    # ========================================[ INITIALIZATION ]========================================
    def __init__(self, network_cfg_path: str, jobs_cfg_path: str = ""):
        self.all_abstract_jobs:  List[NT.JobDescription] = []
        # self.all_data_fragments: List[NT.DataFragment] = []   # moved to internet_network
        self.all_executing_jobs: List[ExecutionJob] = []

        self.internet_network: Optional[NT.InternetNetwork] = None
        self.iteration_num: int = 0

        if jobs_cfg_path == "":
            jobs_cfg_path = network_cfg_path

        self.init_network_from_file(network_cfg_path)
        self.init_jobs_from_file(jobs_cfg_path)


    def init_network_from_file(self, file_path):
        """
        Initializes network nodes from specs file
        """
        required_N_fields = ["total_storage_gb", "network_speed_gbs", "node_name"]
        required_P_fields = ["cpu_cores", "ram_gb", "cpu_speed_multiplier", "node_name"]

        try:
            with open(file_path, "r") as file:
                config = JsonComment().load(file)
        except:
            print("ERROR: Network config file read error.")
            exit(1)

        network_nodes = []
        try:
            for node_data in config["network_nodes"]:
                processing_nodes = []
                for processing_node_data in node_data["processing_nodes"]:
                    amount_of_cpus = processing_node_data["amount_of_cpus"]

                    for i in range(amount_of_cpus):
                        processing_node_args = {
                            field: processing_node_data[field]
                            for field in required_P_fields
                        }

                        processing_node = NT.ProcessingNode(**processing_node_args)     # create individual processing node within center
                        processing_nodes.append(processing_node)

                network_node_args = {
                    field: node_data[field]
                    for field in required_N_fields
                }
                network_node_args["processing_nodes"] = processing_nodes

                network_node = NT.NetworkNode(**network_node_args)          # create network node with its storage and p_nodes
                network_nodes.append(network_node)

            self.internet_network = NT.InternetNetwork(network_nodes)

        except:
            print("ERROR: Wrong network specs file format.")
            exit(1)

    temp_data_locations = {}

    def init_jobs_from_file(self, file_path):
        """
        Initializes tasks from specs file without defining them in simulator
        """
        required_fields = ["num_of_events", "priority", "event_data_size_mb", "io_intensity", "max_parallel_cpus", "time_ms_per_event_per_core", "difficulty_multiplier", "job_name"]

        try:
            with open(file_path, "r") as file:
                config = JsonComment().load(file)
        except:
            print("ERROR: Task config file read error.")
            exit(1)

        jobs = []
        try:
            for job_data in config["jobs"]:             # append all jobs found in json file
                job_args = {field: job_data[field] for field in required_fields}   # create dict from json job specification
                created_job = NT.JobDescription(**job_args)
                jobs.append(created_job)

                temp_all_locations = []

                # read data_location
                data_locations = job_data["data_location"]
                for json_location in data_locations:
                    network_node_name, percentage = json_location           # tuple encoded data location
                    network_node = self.get_network_node_with_name(network_node_name)
                    data_size_gb = int(created_job.total_src_data_size_gb() * percentage)

                    if network_node.malloc(data_size_gb):                   # if there is space
                        created_data_fragment = NT.DataFragment(            # create data fragment, linked to job
                            location=network_node,
                            parent_job_id=created_job.id,
                            data_size_gb=data_size_gb
                        )
                        self.internet_network.all_data_fragments.append(created_data_fragment)       # add it to list

                        temp_all_locations.append([network_node_name, percentage])

                    else:
                        print("ERROR: Invalid data locations")
                        exit(1)
                    
                    self.temp_data_locations[str(job_data["job_name"])] = temp_all_locations

            self.all_abstract_jobs = jobs

        except:
            print("ERROR: Wrong job specs file format.")
            exit(1)

    def define_jobs(self, provided_jobs: List[NT.JobDescription]):
        """Translates job descriptions in simulators execution jobs"""
        for unstarted_task in provided_jobs:
            exec_job = ExecutionJob(unstarted_task)
            self.all_executing_jobs.append(exec_job)


    def start(self):
        self.define_jobs(self.all_abstract_jobs)

        while not self.all_jobs_closed():
            self.iteration()
            if GLOBAL_PARAMS.print_state:
                self.print_state()

        elapsed_time_s = self.iteration_num * GLOBAL_PARAMS.simulation_step_s
        print("\nTOTAL ELAPSED TIME: ", datetime.timedelta(seconds=elapsed_time_s))

        if GLOBAL_PARAMS.print_summary:
            print(f"transfered GB:    {self.internet_network.D_total_transfers_GB}")
            print(f"transfere time s: {self.internet_network.D_total_transfering_time_s}")
        

        print("ok!")


    # ========================================[ ITERATIONS ]========================================

    def all_jobs_closed(self) -> bool:
        """Check if all execution jobs are closed""" 
        for job in self.all_executing_jobs:
            if job.state != ExecutionJob.STATE.CLOSED:
                return False
        return True


    def iteration(self):
        """Simulate one tick of workflow"""
        self.iteration_num += 1
        # print("\nstarted iteration: ", self.iteration_num)

        self.process_running_tasks()    # moved upfront to free p_nodes for new assignments
        self.process_finished_tasks()

        self.process_defined_jobs()
        self.process_assigned_jobs()
        self.process_sending_in_jobs()
        self.process_activated_jobs()

        self.internet_network.simulate_network_tick(GLOBAL_PARAMS.simulation_step_s)
        self.simulate_data_processing()


    def process_defined_jobs(self):
        """Assigns best suited processing node for the job"""
        defined_jobs = self.get_jobs_with_state(ExecutionJob.STATE.DEFINED)

        for job in defined_jobs:
            if self.get_num_active_jobs() >= GLOBAL_PARAMS.max_concurrent_jobs:
                break

            assigned_Nnode = self.get_best_Nnode_for_job(job)

            if assigned_Nnode != -1:
                job.assigned_ProcessingNode = assigned_Nnode.processing_nodes[0]
                job.assigned_NetworkNode = assigned_Nnode
                job.state = ExecutionJob.STATE.ASSIGNED
            else:
                print("ERROR: Could not assign job to any ProcessingNode.")

        
        if GLOBAL_PARAMS.print_summary:
            print("Currenty defined jobs:", len(defined_jobs))

    def process_assigned_jobs(self):
        """Starts all required data transfers to the assigned p_node"""
        assigned_jobs = self.get_jobs_with_state(ExecutionJob.STATE.ASSIGNED)

        for job in assigned_jobs:
            all_job_fragments = self.get_all_data_fragments_for_job_id(job.job_description.id)

            for fragment in all_job_fragments:          # start data transfer
                data_location = fragment.location
                if data_location != job.assigned_NetworkNode:    # fragment needs to be transferred
                    self.internet_network.start_transfer(
                        data=fragment,
                        n_dst=job.assigned_NetworkNode
                    )

            job.state = ExecutionJob.STATE.SENDING_IN

        
        if GLOBAL_PARAMS.print_summary:
            print("Currenty assigned jobs:", len(assigned_jobs))


    def process_sending_in_jobs(self):
        """Checks if all data transfers have completed"""
        sending_jobs = self.get_jobs_with_state(ExecutionJob.STATE.SENDING_IN)

        for job in sending_jobs:
            all_job_fragments = self.get_all_data_fragments_for_job_id(job.job_description.id)

            all_data_on_site = True  # check if all data is on site
            for fragment in all_job_fragments:
                data_location = fragment.location
                if data_location != job.assigned_NetworkNode:
                    all_data_on_site = False
                    break

            if all_data_on_site:
                job.state = ExecutionJob.STATE.ACTIVATED

            
        if GLOBAL_PARAMS.print_summary:
            print("Currenty sending jobs:", len(sending_jobs))

    def process_activated_jobs(self):
        """Checks processing-ready tasks if their assigned p_node is available and starts processing if possible"""
        activated_jobs = self.get_jobs_with_state(ExecutionJob.STATE.ACTIVATED)

        # all jobs that have data on location and are ready for execution (FIFO)
        for job in activated_jobs:
            max_cpus = job.job_description.max_parallel_cpus
            # print(max_cpus)
            max_cpus = 1
            assigned_Pnodes = job.assigned_NetworkNode.allocate_n_Pnodes(max_cpus)
            # if len(assigned_Pnodes) > 0:
            #     print(len(assigned_Pnodes))
            
            job.currently_using_Pnodes = assigned_Pnodes

            # if assigned_Pnode.allocate():      # p_node is free
            if len(assigned_Pnodes) > 0:
                # print(len(assigned_Pnodes))
                job.assigned_NetworkNode.malloc(job.job_description.min_output_space_gb)
                job.start_processing()
            else:
                # wait for node
                pass
            
        if GLOBAL_PARAMS.print_summary:
            print("Currenty activated jobs:", len(activated_jobs))

    def process_running_tasks(self):
        """Checking currently running tasks if any of them is finished"""
        running_jobs = self.get_jobs_with_state(ExecutionJob.STATE.RUNNING)

        for job in running_jobs:
            if job.amount_processed_gb >= job.job_description.total_src_data_size_gb():
                job.state = ExecutionJob.STATE.FINISHED

    def process_finished_tasks(self):
        """Checks if output needs to be transferred"""
        finished_jobs = self.get_jobs_with_state(ExecutionJob.STATE.FINISHED)

        for job in finished_jobs:
            job.assigned_NetworkNode.releaseAll(job.currently_using_Pnodes)
            job.state = ExecutionJob.STATE.CLOSED
        
        if GLOBAL_PARAMS.print_summary:
            print("Currenty finished jobs:", len(finished_jobs))

    def simulate_data_processing(self):
        """Triggers p_node processing ticks"""
        running_jobs = self.get_jobs_with_state(ExecutionJob.STATE.RUNNING)

        for job in running_jobs:
            # check if more cpus are available
            available_Cpus = job.assigned_NetworkNode.currenty_free_cpus()
            # if we can utilise more cpus
            if available_Cpus > 0 and (available_Cpus + len(job.currently_using_Pnodes)) < job.job_description.max_parallel_cpus:
                job.assigned_NetworkNode.releaseAll(job.currently_using_Pnodes)

                assigned_Pnodes = job.assigned_NetworkNode.allocate_n_Pnodes(job.job_description.max_parallel_cpus)
                job.currently_using_Pnodes = assigned_Pnodes
                job.reevaluate_processing_speed()

            job.process_data_tick(GLOBAL_PARAMS.simulation_step_s)


        if GLOBAL_PARAMS.print_summary:
            print("Currenty processing jobs:", len(running_jobs))





    # ========================================[ HELPER FUNCTIONS ]========================================

    def get_num_active_jobs(self) -> int:
        count = 0
        for job in self.all_executing_jobs:
            if job.state not in [ExecutionJob.STATE.DEFINED, ExecutionJob.STATE.CLOSED]:
                count += 1
        return count

    def get_jobs_with_state(self, target_state: ExecutionJob.STATE) -> List[ExecutionJob]:
        """Returns references to jobs with target state"""
        matching_jobs = []
        for job in self.all_executing_jobs:
            if job.state == target_state:
                matching_jobs.append(job)

        return matching_jobs

    def get_processing_node_with_id(self, target_id: int) -> Optional[NT.ProcessingNode]:
        for n_node in self.internet_network.all_nodes:
            for p_node in n_node.processing_nodes:
                if p_node.id == target_id:
                    return p_node
        return None

    def get_network_node_with_id(self, target_id: int) -> Optional[NT.NetworkNode]:
        for n_node in self.internet_network.all_nodes:
            if n_node.id == target_id:
                return n_node
        return None

    def get_job_with_id(self, target_id: int) -> Optional[NT.JobDescription]:
        for job in self.all_abstract_jobs:
            if job.id == target_id:
                return job
        return None

    def get_network_node_with_name(self, target_name: str) -> Optional[NT.NetworkNode]:
        for n_node in self.internet_network.all_nodes:
            if n_node.node_name == target_name:
                return n_node
        return None

    def get_all_data_fragments_for_job_id(self, target_id: int) -> List[NT.DataFragment]:
        matching_fragments = []
        for fragment in self.internet_network.all_data_fragments:
            if fragment.parent_job_id == target_id:
                matching_fragments.append(fragment)

        return matching_fragments




    # ========================================[ SCHEDULER ]========================================
    def get_best_Nnode_for_job(self, job: ExecutionJob) -> NT.NetworkNode:
        """
        Returns best suited node for job, taking job restrictions in account
        """

        best_weight = 0
        best_node = None


        for node in self.internet_network.all_nodes:
            if len(node.processing_nodes) == 0:     # storage only
                continue

            # formula 1
            # estimated_time = job.job_description.time_ms_per_event_per_core * job.job_description.num_of_events
            # estimated_time /= node.processing_nodes[0].cpu_cores * node.processing_nodes[0].cpu_speed_multiplier

            # if estimated_time > node.max_task_cpuTime:
            #     continue

            assigned = 0
            activated = 0
            running = 0
            starting = 0

            for job in self.all_executing_jobs:
                if job.assigned_NetworkNode == node:
                    if job.state == ExecutionJob.STATE.ASSIGNED:
                        assigned += 1
                    elif job.state == ExecutionJob.STATE.ACTIVATED:
                        activated += 1
                    elif job.state == ExecutionJob.STATE.RUNNING:
                        running += 1
                    elif job.state == ExecutionJob.STATE.SENDING_IN:
                        starting += 1


            # formula 2
            manyAssigned = max(1, min(2, assigned/(activated+1e-10)))   # division by 0

            # formula 3
            weight = (running+1) / ((assigned + activated + starting + 10) * manyAssigned)

            # formula 4
            weight *= (node.free_storage_gb + node.total_storage_gb) / node.total_storage_gb
            # print(weight)


            if weight > best_weight:
                best_weight = weight
                best_node = node

        return best_node


    def print_state(self):
        print(f"\n========================[{self.iteration_num}]========================")
        print("Execution Jobs:")
        print('\n'.join(str(task) for task in self.all_executing_jobs if task.state != ExecutionJob.STATE.CLOSED))
        print("--------------------------------")

        # if len(self.sim_data_transfers) > 0:
        #     print("Data transfers:")
        #     print('\n'.join(str(dt) for dt in self.sim_data_transfers))
        #     print("--------------------------------")

        print("Data fragments:")
        print('\n'.join(str(df) for df in self.all_data_fragments))







def printa(arr):
    if arr is None:
        print("[]")
    else:
        print("[", end='')
        for el in arr:
            print(f"{str(el)}, ", end='')
        print("]")



if __name__ == "__main__":
    EE = ExecutionEnvironment(GLOBAL_PARAMS.network_specs_path, GLOBAL_PARAMS.jobs_specs_path)

    EE.start()
