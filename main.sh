#!/bin/bash
source /etc/profile.d/parallelworks.sh
source /etc/profile.d/parallelworks-env.sh
source /pw/.miniconda3/etc/profile.d/conda.sh
conda activate

python input_form_resource_wrapper.py

# The rest of the workflow would go here