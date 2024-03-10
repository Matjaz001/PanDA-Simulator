
from __future__ import annotations
from typing import List, Optional
from pprint import pprint
import random

class IdGen:
    _last_id: int = 0

    @staticmethod
    def next() -> int:
        IdGen._last_id += 1
        return IdGen._last_id


class ProcessingNode:
    def __init__(
            self,
            cpu_cores: int,
            ram_gb: int,
            cpu_speed_multiplier: float,
            node_name: str = "generic"
    ):
        self.id: int = IdGen.next()
        self.cpu_cores: int = cpu_cores
        self.ram_gb: int = ram_gb
        self.cpu_speed_multiplier: float = cpu_speed_multiplier
        self.node_name: str = node_name + " " + str(int(random.random()*100))

        self._is_busy: bool = False
        self.parent_network_node: Optional[NetworkNode] = None


    def allocate(self) -> bool:
        """Start process on this node, locking it for others"""
        if self._is_busy:
            return False
        else:
            self._is_busy = True
            # print("allocated")
            return True

    def release(self) -> None:
        if not self._is_busy:
            print("ERROR: Releasing free pNode")
        self._is_busy = False

    def is_busy(self) -> bool:
        return self._is_busy

    def __str__(self) -> str:
        return f"p_node: {self.node_name}"


class JobDescription:
    def __init__(
            self,
            num_of_events: int,
            priority: int,
            event_data_size_mb: float,  
            io_intensity: float,    # unused
            max_parallel_cpus: int,
            time_ms_per_event_per_core: int,
            difficulty_multiplier: float,

            min_ram_per_core_gb: int = 0,
            min_output_space_gb: int = 0,
            job_name: str = "generic"   # for debug
    ):
        self.id: int = IdGen.next() # prolly unused
        self.priority = 0
        self.num_of_events: int = num_of_events
        self.event_data_size_mb: float = event_data_size_mb
        self.io_intensity: float = io_intensity # unused!
        self.max_parallel_cpus: int = max_parallel_cpus
        self.time_ms_per_event_per_core: int = time_ms_per_event_per_core
        self.difficulty_multiplier: float = difficulty_multiplier

        self.required_data_dst: Optional[NetworkNode] = None
        self.min_ram_per_core_gb: float = min_ram_per_core_gb
        self.min_output_space_gb: int = min_output_space_gb
        self.job_name: str = job_name

    def total_src_data_size_gb(self) -> int:
        return round(self.num_of_events * self.event_data_size_mb / 1000)

    def __str__(self) -> str:
        return f"Job name: {self.job_name}\n" \
               f"Number of Tasks: {self.num_of_events}\n" \
               f"Event Data Size (MB): {self.event_data_size_mb}\n" \
               f"Time ms per Event per Core: {self.time_ms_per_event_per_core}\n" \
               f"Minimum RAM per Core (GB): {self.min_ram_per_core_gb}\n" \
               f"Minimum Output Space (GB): {self.min_output_space_gb}"


class NetworkNode:
    def __init__(
            self,
            processing_nodes: List[ProcessingNode],
            total_storage_gb: int,
            network_speed_gbs: float,
            node_name: str = "generic"
    ):
        self.id: int = IdGen.next()
        self.processing_nodes: List[ProcessingNode] = processing_nodes
        self.total_storage_gb: int = total_storage_gb
        self.network_speed_gbs: float = network_speed_gbs
        self.node_name: str = node_name

        self.free_storage_gb: int = total_storage_gb

        for p_node in processing_nodes:
            p_node.parent_network_node = self

    def allocate_n_Pnodes(self, n: int) -> List[ProcessingNode]:
        allocated_nodes = []

        for pNode in self.processing_nodes:
            if pNode.allocate():
                allocated_nodes.append(pNode)
            if len(allocated_nodes) >= n:
                break
            
        return allocated_nodes

    def malloc(self, amount_gb: int) -> bool:
        """malloc()"""
        if self.free_storage_gb < amount_gb:
            return False
        else:
            self.free_storage_gb -= amount_gb
            return True

    def free(self, amount_gb: int) -> None:
        if self.free == True:
            print("ERROR: Freeing free node!")

        self.free_storage_gb -= amount_gb

        if self.free_storage_gb < 0:
            print(f"ERROR: Negative free storage ({self.free_storage_gb} gb) at {self.node_name}")
            exit(1)

    
    def releaseAll(self, pNodes: List[ProcessingNode]) -> Node:
        for node in pNodes:
            node.release()

    def current_utilization(self) -> float:
        busy_count = 0
        for node in self.processing_nodes:
            if node.is_busy():
                busy_count += 1

        if busy_count == 0:
            return 1
            
        return busy_count/len(self.processing_nodes)

    def currenty_free_cpus(self) -> float:
        free_count = 0
        for node in self.processing_nodes:
            if not node.is_busy():
                free_count += 1
        return free_count


    def __str__(self) -> str:
        processing_nodes_info = '\n'.join(str(node) for node in self.processing_nodes)

        return f"NetworkNode ID: {self.id}\n" \
               f"Node Name: {self.node_name}\n" \
               f"Processing Nodes:\n{processing_nodes_info}\n" \
               f"Total Storage: {self.total_storage_gb} GB\n" \
               f"Free Storage: {self.free_storage_gb} GB\n" \
               f"Network Speed: {self.network_speed_gbs} GB"


class DataFragment:
    def __init__(
            self,
            location: NetworkNode,
            parent_job_id: int,
            data_size_gb: int
    ):
        self.id: int = IdGen.next()

        self.location: NetworkNode = location
        self.parent_job_id: int = parent_job_id
        self.data_size_gb: int = data_size_gb

    def __str__(self) -> str:
        return f"(job id: {self.parent_job_id}, size: {self.data_size_gb} GB, at: {self.location.node_name})"


class DataTransfer:
    def __init__(
            self,
            data: DataFragment,
            n_dst: NetworkNode
    ):
        self.data: DataFragment = data
        self.n_src: NetworkNode = data.location
        self.n_dst: NetworkNode = n_dst

        # self.transfer_speed_gb: float = min(self.n_src.network_speed_gbs, self.n_dst.network_speed_gbs)
        self.transfer_speed_gb:float = 0
        self.amount_transferred_gb: float = 0

        if not self.n_dst.malloc(data.data_size_gb):
            print(f"ERROR: Not enough space on {self.n_dst.node_name}")
            exit(1)

    def __del__(self):
        self.n_src.free(self.data.data_size_gb)


    def __str__(self):
        return f"DataTransfer({self.data.id}, {self.n_src.node_name} -> {self.n_dst.node_name}, " \
               f"at {self.amount_transferred_gb / self.data.data_size_gb})"



class InternetNetwork:
    def __init__(self, all_nodes: List[NetworkNode]):
        self.all_nodes: list[NetworkNode] = all_nodes
        self.all_data_fragments: List[NT.DataFragment] = []
        self.all_data_transfers: List[DataTransfer] = []

        sim_transfer_rates_download = {node: 0 for node in self.all_nodes}  # unused?
        sim_transfer_rates_upload = {node: 0 for node in self.all_nodes}

        self.D_total_transfers_GB = 0
        self.D_total_transfering_time_s = 0


    def start_transfer(self, data: DataFragment, n_dst: NetworkNode):
        self.all_data_transfers.append(DataTransfer(data, n_dst))


    def reevaluate_transfer_speeds(self):
        nTransfers = {node.node_name: 0 for node in self.all_nodes}
        for transfer in self.all_data_transfers:
            nTransfers[transfer.n_src.node_name] += 1
            nTransfers[transfer.n_dst.node_name] += 1

        # print(nTransfers)

        for transfer in self.all_data_transfers:
            max_available_src = transfer.n_src.network_speed_gbs / nTransfers[transfer.n_src.node_name]
            max_available_dst = transfer.n_dst.network_speed_gbs / nTransfers[transfer.n_dst.node_name]

            transfer.transfer_speed_gb = min(max_available_src, max_available_dst)
            # print(transfer.transfer_speed_gb)



    def simulate_network_tick(self, time_step_s: int):
        to_be_removed = []

        self.reevaluate_transfer_speeds()

        for transfer in self.all_data_transfers:
            transfer.amount_transferred_gb += transfer.transfer_speed_gb * time_step_s
            self.D_total_transfers_GB += transfer.transfer_speed_gb * time_step_s
            self.D_total_transfering_time_s += time_step_s

            if transfer.amount_transferred_gb >= transfer.data.data_size_gb:        # all data is transferred
                transfer.data.location = transfer.n_dst
                to_be_removed.append(transfer)

        for el in to_be_removed:
            self.all_data_transfers.remove(el)      # !important for deconstruction

        # print(self.D_total_transfers_GB)










