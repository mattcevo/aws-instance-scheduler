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

import copy
import re

import schedulers
import re
import copy

from boto_retry import get_client_with_retries
from configuration.instance_schedule import InstanceSchedule
from configuration.running_period import RunningPeriod
from configuration.scheduler_config_builder import SchedulerConfigBuilder
from configuration.setbuilders.weekday_setbuilder import WeekdaySetBuilder

from boto3 import boto3

RESTRICTED_REDSHIFT_TAG_VALUE_SET_CHARACTERS = r"[^a-zA-Z0-9\s_\.:+/=\\@-]"

ERR_STARTING_INSTANCE = "Error starting redshift {} {} ({})"
ERR_STOPPING_INSTANCE = "Error stopping redshift {} {}, ({})"
ERR_DELETING_SNAPSHOT = "Error deleting snapshot {}"

INF_ADD_TAGS = "Adding {} tags {} to instance {}"
INF_DELETE_SNAPSHOT = "Deleted previous snapshot {}"
INF_FETCHED = "Number of fetched redshift {} is {}, number of schedulable  resources is {}"
INF_FETCHING_RESOURCES = "Fetching redshift {} for account {} in region {}"
INF_REMOVE_KEYS = "Removing {} key(s) {} from instance {}"
INF_STOPPED_RESOURCE = "Stopped redshift {} \"{}\""
INF_PAUSED_RESOURCE = "Paused redshift {} \"{}\""

DEBUG_READ_REPLICA = "Can not schedule redshift instance \"{}\" because it is a read replica of instance {}"
DEBUG_READ_REPLICA_SOURCE = "Can not schedule redshift instance \"{}\" because it is the source for read copy instance(s) {}"
DEBUG_SKIPPING_INSTANCE = "Skipping redshift {} {} because it is not in a start or stop-able state ({})"
DEBUG_WITHOUT_SCHEDULE = "Skipping redshift {} {} without schedule"
DEBUG_SELECTED = "Selected redshift instance {} in state ({}) for schedule {}"
DEBUG_NO_SCHEDULE_TAG = "Instance {} has no schedule tag named {}"

WARN_TAGGING_STARTED = "Error setting start or stop tags to started instance {}, ({})"
WARN_TAGGING_STOPPED = "Error setting start or stop tags to stopped instance {}, ({})"
WARN_REDSHIFT_TAG_VALUE = "Tag value \"{}\" for tag \"{}\" changed to \"{}\" because it did contain characters that are not allowed " \
                    "in REDSHIFT tag values. The value can only contain only the set of Unicode letters, digits, " \
                     "white-space, '_', '.', '/', '=', '+', '-'"

MAINTENANCE_SCHEDULE_NAME = "REDSHIFT preferred Maintenance Window Schedule"
MAINTENANCE_PERIOD_NAME = "REDSHIFT preferred Maintenance Window Period"


class RedShiftService:
    REDSHIFT_STATE_AVAILABLE = "available"
    REDSHIFT_STATE_STOPPED = "stopped"
    REDSHIFT_STATE_PAUSED = "paused"

    REDSHIFT_SCHEDULABLE_STATES = {REDSHIFT_STATE_AVAILABLE, REDSHIFT_STATE_STOPPED, REDSHIFT_STATE_PAUSED}

    def __init__(self):
        self.service_name = "redshift"
        self.allow_resize = False
        self._instance_tags = None

        self._context = None
        self._session = None
        self._region = None
        self._account = None
        self._logger = None
        self._tagname = None
        self._stack_name = None
        self._config = None

    def _init_scheduler(self, args):
        """
        Initializes common parameters
        :param args: action parameters
        :return:
        """
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._region = args.get(schedulers.PARAM_REGION)
        self._stack_name = args.get(schedulers.PARAM_STACK)
        self._session = args.get(schedulers.PARAM_SESSION)
        self._tagname = args.get(schedulers.PARAM_CONFIG).tag_name
        self._config = args.get(schedulers.PARAM_CONFIG)
        self._instance_tags = None

    @property
    def redshift_resource_tags(self):

        # print ('redshift_resource_tags self : {}', self)
        # print ('redshift_resource_tags self._instance_tags : {}', self._instance_tags)

        if self._instance_tags is None:
            tag_client = get_client_with_retries("resourcegroupstaggingapi",
                                                 methods=["get_resources"],
                                                 session=self._session,
                                                 context=self._context,
                                                 region=self._region)

            # args = {
            #     "TagFilters": [{"Key": self._tagname}],
            #     "ResourcesPerPage": 50,
            #     "ResourceTypeFilters": ["redshift:db", "redshift:cluster"]
            # }
            args = {
                "TagFilters": [{"Key": self._tagname}],
                "ResourcesPerPage": 50
            }

            self._instance_tags = {}
            # print ('redshift_resource_tags self._instance_tags 2: {}', self._instance_tags)

            while True:
                resp = tag_client.get_resources_with_retries(**args)
                # print ('redshift_resource_tags self._instance_tags 2: {}', self._instance_tags)

                for resource in resp.get("ResourceTagMappingList", []):
                    self._instance_tags[resource["ResourceARN"]] = {tag["Key"]: tag["Value"]
                                                                    for tag in resource.get("Tags", {})
                                                                    if tag["Key"] in ["Name", self._tagname]}

                if resp.get("PaginationToken", "") != "":
                    args["PaginationToken"] = resp["PaginationToken"]
                else:
                    break

        return self._instance_tags

    @staticmethod
    def build_schedule_from_maintenance_window(period_str):
        """
        Builds a Instance running schedule based on an REDSHIFT preferred maintenance windows string in format ddd:hh:mm-ddd:hh:mm
        :param period_str: redshift maintenance windows string
        :return: Instance running schedule with timezone UTC
        """

        # get elements of period
        start_string, stop_string = period_str.split("-")
        start_day_string, start_hhmm_string = start_string.split(":", 1)
        stop_day_string, stop_hhmm_string = stop_string.split(":", 1)

        # weekday set builder
        weekdays_builder = WeekdaySetBuilder()

        start_weekday = weekdays_builder.build(start_day_string)
        start_time = SchedulerConfigBuilder.get_time_from_string(start_hhmm_string)
        end_time = SchedulerConfigBuilder.get_time_from_string(stop_hhmm_string)

        # windows with now day overlap, can do with one period for schedule
        if start_day_string == stop_day_string:
            periods = [
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME,
                                            begintime=start_time,
                                            endtime=end_time,
                                            weekdays=start_weekday)
                }]
        else:
            # window with day overlap, need two periods for schedule
            end_time_day1 = SchedulerConfigBuilder.get_time_from_string("23:59")
            begin_time_day2 = SchedulerConfigBuilder.get_time_from_string("00:00")
            stop_weekday = weekdays_builder.build(stop_day_string)
            periods = [
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME + "-{}".format(start_day_string),
                                            begintime=start_time,
                                            endtime=end_time_day1,
                                            weekdays=start_weekday),
                    "instancetype": None
                },
                {
                    "period": RunningPeriod(name=MAINTENANCE_PERIOD_NAME + "-{}".format(stop_day_string),
                                            begintime=begin_time_day2,
                                            endtime=end_time,
                                            weekdays=stop_weekday),
                    "instancetype": None
                }]

        # create schedule with period(s) and timezone UTC
        schedule = InstanceSchedule(name=MAINTENANCE_SCHEDULE_NAME, periods=periods, timezone="UTC", enforced=True)

        return schedule

    def get_schedulable_resources(self, fn_is_schedulable, fn_describe_name, kwargs):

        self._init_scheduler(kwargs)

        client = get_client_with_retries("redshift", [fn_describe_name], context=self._context, session=self._session,
                                         region=self._region)

        describe_arguments = {}
        resource_name = fn_describe_name.split("_")[-1]
        resource_name = resource_name[0].upper() + resource_name[1:]
        resources = []
        number_of_resources = 0
        self._logger.info(INF_FETCHING_RESOURCES, resource_name, self._account, self._region)

        print ('redshift service type: ', type(client))

        while True:
            self._logger.debug("Making {} call with parameters {}", fn_describe_name, describe_arguments)
            fn = getattr(client, fn_describe_name + "_with_retries")
            redshift_resp = fn(**describe_arguments)
            # for resource in redshift_resp["DB" + resource_name]:
            for resource in redshift_resp[resource_name]:
                number_of_resources += 1

                if fn_is_schedulable(resource):

                    resource_data = self._select_resource_data(redshift_resource=resource, is_cluster=resource_name == "Clusters")

                    schedule_name = resource_data[schedulers.INST_SCHEDULE]
                    if schedule_name not in [None, ""]:
                        self._logger.debug(DEBUG_SELECTED, resource_data[schedulers.INST_ID],
                                           resource_data[schedulers.INST_STATE_NAME],
                                           schedule_name)
                        resources.append(resource_data)
                    else:
                        self._logger.debug(DEBUG_WITHOUT_SCHEDULE, resource_name[:-1], resource_data[schedulers.INST_ID])
            if "Marker" in redshift_resp:
                describe_arguments["Marker"] = redshift_resp["Marker"]
            else:
                break
        self._logger.info(INF_FETCHED, resource_name, number_of_resources, len(resources))
        return resources

    def get_schedulable_redshift_instances(self, kwargs):

        def is_schedulable_instance(redshift_inst):
            # db_id = redshift_inst["DBInstanceIdentifier"]
            cluster_id = redshift_inst["ClusterIdentifier"]
            # print ('redshift_inst : {}', str(redshift_inst))

            # state = redshift_inst["DBInstanceStatus"]
            state = redshift_inst["ClusterStatus"]

            if state not in RedShiftService.REDSHIFT_SCHEDULABLE_STATES:
                self._logger.debug(DEBUG_SKIPPING_INSTANCE, "instance", cluster_id, state)
                return False

            # not applicable for redshift
            # if redshift_inst.get("ReadReplicaSourceDBInstanceIdentifier", None) is not None:
            #     self._logger.debug(DEBUG_READ_REPLICA, cluster_id, redshift_inst["ReadReplicaSourceDBInstanceIdentifier"])
            #     return False

            # if len(redshift_inst.get("ReadReplicaDBInstanceIdentifiers", [])) > 0:
            #     self._logger.debug(DEBUG_READ_REPLICA_SOURCE, cluster_id, ",".join(redshift_inst["ReadReplicaDBInstanceIdentifiers"]))
            #     return False

            # if redshift_inst["Engine"] in ["aurora"]:
            #     return False

            # print ('\nself.redshift_resource_tags : {}', self.redshift_resource_tags)
            # print ('\ntype(self.redshift_resource_tags) : {}', type(self.redshift_resource_tags))
            # print ('\nredshift_inst : {}', redshift_inst)
            # print ('\nself.redshift_resource_tags.get(redshift_inst["cluster"]) : {}', self.redshift_resource_tags.get(redshift_inst["cluster"]))

            print('\nredshift_inst.get("ClusterIdentifier") : {} ', redshift_inst.get("ClusterIdentifier"))


            # Using items() + list comprehension 
            # Substring Key match in dictionary 
            # res = [val for key, val in test_dict.items() if search_key in key] 
            res = [val for key, val in self.redshift_resource_tags.items() if redshift_inst.get("ClusterIdentifier") in key]
            # res = [val for key, val in self.redshift_resource_tags.items() if 'matth-redshift-cluster-2' in key]

            # printing result  
            # print("\n\nValues for substring keys : " + str(res))
            # print("\n\neval res is none : {}", res is None)

            # if [val for key, val in self.redshift_resource_tags.items() if 'matth-redshift-cluster-2' in key] is None:

            # checking to see if there are any 'schedule' tags on any of the redshift clusters
            if [val for key, val in self.redshift_resource_tags.items() if redshift_inst.get("ClusterIdentifier") in key] is None:
                self._logger.debug(DEBUG_NO_SCHEDULE_TAG, redshift_inst, self._tagname)
                return False

            return True

        return self.get_schedulable_resources(fn_is_schedulable=is_schedulable_instance,
                                            #   fn_describe_name="describe_db_instances",
                                                fn_describe_name="describe_clusters",
                                              kwargs=kwargs)

    def get_schedulable_redshift_clusters(self, kwargs):
        def is_schedulable(cluster_inst):

            db_id = cluster_inst["DBClusterIdentifier"]     # don't know if these exist in a redshift context...
            # db_id = redshift_inst["DBInstanceIdentifier"]
            # cluster_id = redshift_inst["ClusterIdentifier"]

            state = cluster_inst["Status"]

            if state not in RedShiftService.REDSHIFT_SCHEDULABLE_STATES:
                self._logger.debug(DEBUG_SKIPPING_INSTANCE, "cluster", db_id, state)
                return False

            if self.redshift_resource_tags.get(cluster_inst["DBClusterArn"]) is None:
                self._logger.debug(DEBUG_NO_SCHEDULE_TAG, cluster_inst, self._tagname)
                return False

            return True

        return self.get_schedulable_resources(fn_is_schedulable=is_schedulable,
                                              fn_describe_name="describe_db_clusters",
                                              kwargs=kwargs)

    def get_schedulable_instances(self, kwargs):
        instances = self.get_schedulable_redshift_instances(kwargs)
        if self._config.schedule_clusters:
            instances += self.get_schedulable_redshift_clusters(kwargs)
        return instances

    def _select_resource_data(self, redshift_resource, is_cluster):

        print ('\nis_cluster : {}', is_cluster)
        print ('\nredshift_resource : {}', redshift_resource)

        # arn_for_tags = redshift_resource["DBInstanceArn"] if not is_cluster else redshift_resource["DBClusterArn"]
        arn_for_tags = redshift_resource["ClusterIdentifier"] if not is_cluster else redshift_resource["ClusterIdentifier"]
        print ('\narn_for_tags : {}', arn_for_tags)

        # tags = self.redshift_resource_tags.get(arn_for_tags, {})
        tags = [val for key, val in self.redshift_resource_tags.items() if redshift_resource.get("ClusterIdentifier") in key]
        # print ('\nself.redshift_resource_tags : {}', str(self.redshift_resource_tags))
        # print ('\ntags : {}', tags)
        # print ('\njtags : {}', jtags)
        # print ('\ntype(jtags) : {}', type(jtags))
        # print ('\ntags[0] : {}', tags[0])
        # print ('\ntype(tags[0]) : {}', type(tags[0]))
        # print ('\ntags[0].get("Schedule") : ', tags[0].get('Schedule'))

        # state = redshift_resource["DBInstanceStatus"] if not is_cluster else redshift_resource["Status"]
        state = redshift_resource["ClusterStatus"] # if not is_cluster else redshift_resource["Status"]
        print ('\nstate : {}', str(state))

        is_running = state == self.REDSHIFT_STATE_AVAILABLE

        instance_data = {
            # schedulers.INST_ID: redshift_resource["DBInstanceIdentifier"] if not is_cluster else redshift_resource["DBClusterIdentifier"],
            schedulers.INST_ID: redshift_resource["ClusterIdentifier"] if not is_cluster else redshift_resource["ClusterIdentifier"],
            # schedulers.INST_ARN: redshift_resource["DBInstanceArn"] if not is_cluster else redshift_resource["DBClusterArn"],
            schedulers.INST_ARN: redshift_resource["ClusterIdentifier"] if not is_cluster else redshift_resource["ClusterIdentifier"],
            schedulers.INST_ALLOW_RESIZE: self.allow_resize,
            schedulers.INST_HIBERNATE: False,
            schedulers.INST_STATE: state,
            schedulers.INST_STATE_NAME: state,
            schedulers.INST_IS_RUNNING: is_running,
            schedulers.INST_IS_TERMINATED: False,
            schedulers.INST_CURRENT_STATE: InstanceSchedule.STATE_RUNNING if is_running else InstanceSchedule.STATE_STOPPED,
            # schedulers.INST_INSTANCE_TYPE: redshift_resource["DBInstanceClass"] if not is_cluster else "cluster",
            # schedulers.INST_ENGINE_TYPE: redshift_resource["Engine"],
            schedulers.INST_MAINTENANCE_WINDOW: RedShiftService.build_schedule_from_maintenance_window(
                redshift_resource["PreferredMaintenanceWindow"]),
            schedulers.INST_TAGS: tags,
            # schedulers.INST_NAME: tags.get("Name", ""),
            schedulers.INST_NAME: "Schedule",
            # schedulers.INST_SCHEDULE: tags.get(self._tagname, None),
            schedulers.INST_SCHEDULE: tags[0].get('Schedule'),
            schedulers.INST_DB_IS_CLUSTER: is_cluster
        }

        print ('\ninstance_data : {}', str(instance_data))

        return instance_data

    def resize_instance(self, kwargs):
        pass

    def _validate_redshift_tag_values(self, tags):
        result = copy.deepcopy(tags)
        for t in result:
            original_value = t.get("Value", "")
            value = re.sub(RESTRICTED_REDSHIFT_TAG_VALUE_SET_CHARACTERS, " ", original_value)
            value = value.replace("\n", " ")
            if value != original_value:
                self._logger.warning(WARN_REDSHIFT_TAG_VALUE, original_value, t, value)
                t["Value"] = value
        return result

    def _stop_instance(self, client, inst):
        print ('\n#############################################')
        print ('redshift_service: _stop_instance')
        print ('#############################################\n')
        # this will actually attempt to pause the instance
        
        # def does_snapshot_exist(name):

        #     try:
        #         resp = client.describe_db_snapshots_with_retries(DBSnapshotIdentifier=name, SnapshotType="manual")
        #         snapshot = resp.get("DBSnapshots", None)
        #         return snapshot is not None
        #     except Exception as ex:
        #         if type(ex).__name__ == "DBSnapshotNotFoundFault":
        #             return False
        #         else:
        #             raise ex

        print ('\n\nclient : ', client)
        print ('\ninst : ', inst)

        args = {
            "DBInstanceIdentifier": inst.id
        }

        print ('\nargs : ', args)

        # if self._config.create_redshift_snapshot:
        #     snapshot_name = "{}-stopped-{}".format(self._stack_name, inst.id).replace(" ", "")
        #     args["DBSnapshotIdentifier"] = snapshot_name

        #     try:
        #         if does_snapshot_exist(snapshot_name):
        #             client.delete_db_snapshot_with_retries(DBSnapshotIdentifier=snapshot_name)
        #             self._logger.info(INF_DELETE_SNAPSHOT, snapshot_name)
        #     except Exception as ex:
        #         self._logger.error(ERR_DELETING_SNAPSHOT, snapshot_name)

        try:
            # client = get_client_with_retries("redshift", [fn_describe_name], context=self._context, session=self._session,region=self._region)

            print ('\ntype(client) : ', type(client))

            client.stop_db_instance_with_retries(**args)
            self._logger.info(INF_STOPPED_RESOURCE, "instance", inst.id)
        except Exception as ex:
            self._logger.error(ERR_STOPPING_INSTANCE, "instance", inst.instance_str, str(ex))

    def _tag_stopped_resource(self, client, redshift_resource):
        print ('#############################################')
        print ('redshift_service: _tag_stopped_resource')
        print ('#############################################\n')

        stop_tags = self._validate_redshift_tag_values(self._config.stopped_tags)
        if stop_tags is None:
            stop_tags = []
        stop_tags_key_names = [t["Key"] for t in stop_tags]

        start_tags_keys = [t["Key"] for t in self._config.started_tags if t["Key"] not in stop_tags_key_names]

        try:
            if start_tags_keys is not None and len(start_tags_keys):
                self._logger.info(INF_REMOVE_KEYS, "start",
                                  ",".join(["\"{}\"".format(k) for k in start_tags_keys]), redshift_resource.arn)
                client.remove_tags_from_resource_with_retries(ResourceName=redshift_resource.arn, TagKeys=start_tags_keys)
            if len(stop_tags) > 0:
                self._logger.info(INF_ADD_TAGS, "stop", str(stop_tags), redshift_resource.arn)
                client.add_tags_to_resource_with_retries(ResourceName=redshift_resource.arn, Tags=stop_tags)
        except Exception as ex:
            self._logger.warning(WARN_TAGGING_STOPPED, redshift_resource.id, str(ex))

    def _tag_started_instances(self, client, redshift_resource):

        start_tags = self._validate_redshift_tag_values(self._config.started_tags)
        if start_tags is None:
            start_tags = []
        start_tags_key_names = [t["Key"] for t in start_tags]

        stop_tags_keys = [t["Key"] for t in self._config.stopped_tags if t["Key"] not in start_tags_key_names]
        try:
            if stop_tags_keys is not None and len(stop_tags_keys):
                self._logger.info(INF_REMOVE_KEYS, "stop",
                                  ",".join(["\"{}\"".format(k) for k in stop_tags_keys]), redshift_resource.arn)
                client.remove_tags_from_resource_with_retries(ResourceName=redshift_resource.arn, TagKeys=stop_tags_keys)
            if start_tags is not None and len(start_tags) > 0:
                self._logger.info(INF_ADD_TAGS, "start", str(start_tags), redshift_resource.arn)
                client.add_tags_to_resource_with_retries(ResourceName=redshift_resource.arn, Tags=start_tags)
        except Exception as ex:
            self._logger.warning(WARN_TAGGING_STARTED, redshift_resource.id, str(ex))

    # noinspection PyMethodMayBeStatic
    def stop_instances(self, kwargs):
        self._logger.info('#############################################')
        self._logger.info('redshift_service: stop_instances')
        self._logger.info('#############################################\n')

        # print ('kwargs : ', kwargs)

        self._init_scheduler(kwargs)

        # methods = ["stop_db_instance",
        #            "stop_db_cluster",
        #            "describe_db_snapshots",
        #            "delete_db_snapshot",
        #            "add_tags_to_resource",
        #            "remove_tags_from_resource"]

        methods = ["pause_cluster"]

        client = get_client_with_retries("redshift", methods, context=self._context, session=self._session, region=self._region)

        stopped_instances = kwargs["stopped_instances"]
        self._logger.info('stopped_instances : {} \n', stopped_instances)

        for redshift_resource in stopped_instances:
            # print ('\nredshift_resource : \n', redshift_resource, '\n')
            self._logger.info('redshift_resource : {} ', str(redshift_resource))
            try:
                if redshift_resource.is_cluster:

                    # client = boto3.client('redshift')
                    self._logger.info('type(client) : {}', type(client))
                    self._logger.info('boto3.__version__ : {}', boto3.__version__)

                    # client.stop_db_cluster_with_retries(DBClusterIdentifier=redshift_resource.id)
                    print ('ClusterIdentifier=redshift_resource.id : ', redshift_resource.id)
                    client.pause_cluster_with_retries(ClusterIdentifier=redshift_resource.id)
                    # client.pause_cluster(redshift_resource.id)

                    self._logger.info('boto3.__version__ : {}', boto3.__version__)

                    # self._logger.info(INF_STOPPED_RESOURCE, "cluster", redshift_resource.id)
                    self._logger.info(INF_PAUSED_RESOURCE, "cluster", redshift_resource.id)
                else:
                    self._stop_instance(client, redshift_resource)

                self._tag_stopped_resource(client, redshift_resource)

                yield redshift_resource.id, InstanceSchedule.STATE_STOPPED
            except Exception as ex:
                self._logger.error(ERR_STOPPING_INSTANCE, "cluster" if redshift_resource.is_cluster else "instance",
                                   redshift_resource.instance_str, str(ex))

    # noinspection PyMethodMayBeStatic
    def start_instances(self, kwargs):
        self._init_scheduler(kwargs)

        methods = ["start_db_instance",
                   "start_db_cluster",
                   "add_tags_to_resource",
                   "remove_tags_from_resource"]

        client = get_client_with_retries("redshift", methods, context=self._context, session=self._session, region=self._region)

        started_instances = kwargs["started_instances"]
        for redshift_resource in started_instances:

            try:
                if redshift_resource.is_cluster:
                    client.start_db_cluster_with_retries(DBClusterIdentifier=redshift_resource.id)
                else:
                    client.start_db_instance_with_retries(DBInstanceIdentifier=redshift_resource.id)

                self._tag_started_instances(client, redshift_resource)

                yield redshift_resource.id, InstanceSchedule.STATE_RUNNING
            except Exception as ex:
                self._logger.error(ERR_STARTING_INSTANCE, "cluster" if redshift_resource.is_cluster else "instance",
                                   redshift_resource.instance_str, str(ex))
                return
