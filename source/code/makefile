######################################################################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/                                                                               #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

# locally running call to deploy
# make deploy bucket=matth-aws-instance-scheduler-590312749310 solution=matt-aws-instance-scheduler version=`cat version.txt`

# hardcoded region to get cfn template to run successfully
s3bucket=s3://$(bucket)-ap-southeast-2/

pydirs= configuration configuration/setbuilders requesthandlers schedulers util boto_retry
py=$(foreach dir, $(pydirs), $(wildcard $(dir)/*.py))

cli_dirs = cli cli/scheduler_cli
pycli=$(foreach d, $(cli_dirs), $(wildcard $(d)/*.py))

deployment=../../deployment

global_assets_dir=$(deployment)/global-s3-assets
regional_assets_dir=$(deployment)/regional-s3-assets

zip = $(regional_assets_dir)/instance-scheduler.zip
zip_cli = $(regional_assets_dir)/scheduler-cli.zip

# instance scheduler templates
templates=$(wildcard ../../deployment/*.template)

pwd=`pwd`

build: lambda scheduler-cli  cfn

######################################################################################################################
# lambda code                                                                                                        #
######################################################################################################################

lambda:$(py) main.py version.txt

	mkdir -p $(regional_assets_dir)

	# delete old zip files
	find $(deployment) -maxdepth 1 -type f -name 'instance-scheduler.zip' -delete

	zip  $(zip) version.txt
	mv version.py version.py.org
	sed s/%version%/$(version)/g version.py.org > version.py
	zip  $(zip) main.py version.py
	rm version.py
	mv version.py.org version.py
	zip -r $(zip)  $(py)  version.txt
	zip -r $(zip) models
	zip -r $(zip) pytz
	zip -r $(zip) requests
	zip -r $(zip) urllib3
	zip -r $(zip) chardet
	zip -r $(zip) idna
	zip -r $(zip) certifi
	zip -r $(zip) boto3
	zip -r $(zip) botocore
######################################################################################################################
# cli                                                                                                                #
######################################################################################################################

scheduler-cli:$(pycli) version.txt

	# delete old deployments
	find $(deployment) -maxdepth 1 -type f -name 'scheduler-cli.zip' -delete

	mkdir -p $(deployment)

	mv cli/scheduler_cli/scheduler_cli.py cli/scheduler_cli/scheduler_cli.org
	sed s/#version#/$(version)/g cli/scheduler_cli/scheduler_cli.org > cli/scheduler_cli/scheduler_cli.py
	cp version.txt cli/version.txt
	cd ./cli && zip -r ../$(zip_cli) scheduler_cli/*.py
	rm cli/scheduler_cli/scheduler_cli.py
	mv cli/scheduler_cli/scheduler_cli.org cli/scheduler_cli/scheduler_cli.py
	mv cli/setup.py cli/setup.org
	sed s/#version#/$(version)/g cli/setup.org > cli/setup.py
	cd ./cli && zip ../$(zip_cli) setup.py instance-scheduler-cli-runner.py
	rm cli/version.txt
	rm cli/setup.py
	mv cli/setup.org cli/setup.py

	# 'creating' missing '-latest' version...
	cp $(regional_assets_dir)/scheduler-cli.zip $(deployment)/scheduler-cli-latest.zip

######################################################################################################################
# cloudformation templates                                                                                           #
######################################################################################################################

cfn:version.txt $(templates)
	mkdir -p $(deployment)
	mkdir -p $(global_assets_dir)

	# delete old templates
	find $(global_assets_dir) -maxdepth 1 -type f -name 'instance-scheduler*.template' -delete

	# build cfn templates
	python ./build-instance-scheduler-template.py ../cloudformation/instance-scheduler.template  $(bucket) $(solution) $(version) > $(global_assets_dir)/instance-scheduler.template
	python ./build-instance-scheduler-template.py ../cloudformation/instance-scheduler-remote.template  $(bucket) $(solution) $(version) > $(global_assets_dir)/instance-scheduler-remote.template

	# new lines to fix deployment
	cp $(global_assets_dir)/instance-scheduler.template $(deployment)/instance-scheduler-latest.template
	cp $(global_assets_dir)/instance-scheduler-remote.template $(deployment)/instance-scheduler-remote-latest.template

######################################################################################################################
# Deploy to S3                                                                                                       #
######################################################################################################################

deploy: lambda cfn scheduler-cli
	# removing the public-read acl flag
	# aws s3 cp $(deployment)/instance-scheduler-latest.template $(s3bucket)$(prefix)  --acl public-read
	# aws s3 cp $(deployment)/instance-scheduler-latest.template $(s3bucket)  --acl public-read

	# aws s3 cp $(deployment)/instance-scheduler-remote-latest.template $(s3bucket)$(prefix)  --acl public-read
	# aws s3 cp $(deployment)/instance-scheduler-remote-latest.template $(s3bucket)  --acl public-read

	# aws s3 cp $(zip) $(s3bucket)$/$(prefix)  --acl public-read
	# aws s3 cp $(zip_cli) $(s3bucket)$(prefix) --acl public-read
	# aws s3 cp $(deployment)/scheduler-cli-latest.zip $(s3bucket) --acl public-read

	aws s3 cp $(deployment)/instance-scheduler-latest.template $(s3bucket)$(prefix)
	aws s3 cp $(deployment)/instance-scheduler-latest.template $(s3bucket)

	aws s3 cp $(deployment)/instance-scheduler-remote-latest.template $(s3bucket)$(prefix)
	aws s3 cp $(deployment)/instance-scheduler-remote-latest.template $(s3bucket)

	aws s3 cp $(zip) $(s3bucket)$/$(prefix)
	aws s3 cp $(zip_cli) $(s3bucket)$(prefix)
	aws s3 cp $(deployment)/scheduler-cli-latest.zip $(s3bucket)
	
	python update-build-number.py version.txt
