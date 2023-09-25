import os
import json
from typing import List

BUCKETS: List = json.loads(os.getenv('GCS_BUCKETS'))