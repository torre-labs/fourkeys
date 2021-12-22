#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

environment(){
	# If env.sh exists, use values in there
	DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
	[[ -f "$DIR/env.sh" ]] && echo "Importing environment from $DIR/env.sh..." && . $DIR/env.sh

	if [[ ! ${FOURKEYS_PROJECT} ]]
	# If env.sh does not exist, use current active project
	then FOURKEYS_PROJECT=$(gcloud config get-value project)
	fi
}

project_prompt(){
	# Confirm project is the correct one to use for four-keys
	continue=1
	while [[ ${continue} -gt 0 ]]
	do

	# Prompt until project-id is correct
	read -p "Would you like to use ${FOURKEYS_PROJECT} to deploy a new Cloud Run worker? (y/n) :" yesno

	if [[ ${yesno} == "y" ]]
	then continue=0
	else read -p "Please input project_id: " projectid
	export FOURKEYS_PROJECT=${projectid}
	fi

	done
}

source_prompt(){
	# Will be used to name the Cloud Run service and pubsub topic. Should be lowercase.
	read -p "What is the nickname of your source?  Eg github (lowercase):  " nickname
}

build_deploy_cloud_run(){
	# Build and deploy by copying new_source_template
	echo "Creating ${nickname}-worker"
  cp -R $DIR/../bq-workers/new-source-template $DIR/../bq-workers/${nickname}-parser

	cd $DIR/../bq-workers/${nickname}-parser
	gcloud builds submit $DIR/../bq-workers/${nickname}-parser --tag=gcr.io/${FOURKEYS_PROJECT}/${nickname}-parser --project=${FOURKEYS_PROJECT}

  cd $DIR
  sed -i.bak -E "s/^parsers = \[(.*)\]/parsers = \[\1, \"${nickname}\"\]/" terraform.tfvars
  terraform apply
}

environment
project_prompt
source_prompt
build_deploy_cloud_run
