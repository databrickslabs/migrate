import ast
import json
import os
import time
from datetime import timedelta
from timeit import default_timer as timer

from dbclient import *


class DbfsClient(ClustersClient):

    @staticmethod
    def get_num_of_lines(fname):
        if not os.path.exists(fname):
            return 0
        else:
            i = 0
            with open(fname) as fp:
                for line in fp:
                    i += 1
            return i

    def export_dbfs_mounts(self):
        # check if instance profile exists, ask users to use --users first or enter yes to proceed.
        start = timer()
        cid = self.launch_cluster()
        end = timer()
        print("Cluster creation time: " + str(timedelta(seconds=end - start)))
        time.sleep(5)
        ec_id = self.get_execution_context(cid)

        # get all dbfs mount metadata
        dbfs_mount_logfile = self._export_dir + 'dbfs_mounts.log'
        all_mounts_cmd = 'all_mounts = [{"path": x.mountPoint, "source": x.source, ' \
                                        '"encryptionType": x.encryptionType} for x in dbutils.fs.mounts()]'
        results = self.submit_command(cid, ec_id, all_mounts_cmd)
        results = self.submit_command(cid, ec_id, 'print(len(all_mounts))')
        # grab the number of mounts to bucket / batch the export
        num_of_mounts = ast.literal_eval(results['data'])

        batch_size = 100    # batch size to iterate over databases
        num_of_buckets = (num_of_mounts // batch_size) + 1     # number of slices of the list to take

        with open(dbfs_mount_logfile, 'w') as fp_log:
            for m in range(0, num_of_buckets):
                mounts_slice = 'print(all_mounts[{0}:{1}])'.format(batch_size*m, batch_size*(m+1))
                results = self.submit_command(cid, ec_id, mounts_slice)
                mounts_slice_data = ast.literal_eval(results['data'])
                for mount_path in mounts_slice_data:
                    print("Mounts: {0}".format(mount_path))
                    fp_log.write(json.dumps(mount_path))
                    fp_log.write('\n')
        return True
