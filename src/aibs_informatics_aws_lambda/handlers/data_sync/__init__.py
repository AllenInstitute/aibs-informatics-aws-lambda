__all__ = [
    "get_json_from_file_handler",
    "put_json_to_file_handler",
    "batch_data_sync_handler",
    "data_sync_handler",
    "prepare_batch_data_sync_handler",
    "get_data_path_stats_handler",
    "list_data_paths_handler",
    "outdated_data_path_scanner_handler",
    "remove_data_paths_handler",
]

from aibs_informatics_aws_lambda.handlers.data_sync.file_system import (
    GetDataPathStatsHandler,
    ListDataPathsHandler,
    OutdatedDataPathScannerHandler,
    RemoveDataPathsHandler,
)
from aibs_informatics_aws_lambda.handlers.data_sync.operations import (
    BatchDataSyncHandler,
    DataSyncHandler,
    GetJSONFromFileHandler,
    PrepareBatchDataSyncHandler,
    PutJSONToFileHandler,
)

get_json_from_file_handler = GetJSONFromFileHandler.get_handler()
put_json_to_file_handler = PutJSONToFileHandler.get_handler()
batch_data_sync_handler = BatchDataSyncHandler.get_handler()
data_sync_handler = DataSyncHandler.get_handler()
prepare_batch_data_sync_handler = PrepareBatchDataSyncHandler.get_handler()

get_data_path_stats_handler = GetDataPathStatsHandler.get_handler()
list_data_paths_handler = ListDataPathsHandler.get_handler()
outdated_data_path_scanner_handler = OutdatedDataPathScannerHandler.get_handler()
remove_data_paths_handler = RemoveDataPathsHandler.get_handler()
