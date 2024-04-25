import sys
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['s3_target_path_key','s3_target_path_bucket'])

bucket = args['s3_target_path_bucket']
filename = args['s3_target_path_key']