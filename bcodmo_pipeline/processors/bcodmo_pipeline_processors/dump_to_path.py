import os
import shutil

from dataflows import Flow
from dataflows.processors.dumpers.file_dumper import FileDumper
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.stat_utils import STATS_DPP_KEY, STATS_OUT_DP_URL_KEY

class PathDumper(FileDumper):

    def __init__(self, out_path='.', **options):
        super(PathDumper, self).__init__(options)
        self.out_path = out_path
        PathDumper.__makedirs(self.out_path)

    def write_file_to_output(self, filename, path):
        path = os.path.join(self.out_path, path)
        # Avoid rewriting existing files
        if self.add_filehash_to_path and os.path.exists(path):
            return
        path_part = os.path.dirname(path)
        PathDumper.__makedirs(path_part)
        shutil.copy(filename, path)
        # Change file permissions to 777
        os.chmod(path, 0o775)
        return path

    @staticmethod
    def __makedirs(path):
        os.makedirs(path, exist_ok=True)




def flow(parameters: dict, stats: dict):
    out_path = parameters.pop('out-path', '.')
    stats.setdefault(STATS_DPP_KEY, {})[STATS_OUT_DP_URL_KEY] = os.path.join(out_path, 'datapackage.json')
    return Flow(
        PathDumper(
            out_path,
            **parameters
        )
    )



if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters, ctx.stats), ctx)
