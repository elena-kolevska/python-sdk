# -*- coding: utf-8 -*-
# Copyright 2023 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta
from time import sleep
from dapr.ext.workflow import (
    WorkflowRuntime,
    DaprWorkflowContext,
    WorkflowActivityContext,
    RetryPolicy,
    DaprWorkflowClient,
)
from dapr.conf import Settings
from dapr.clients import DaprClient
from dapr.clients.exceptions import DaprInternalError

settings = Settings()

counter = 0
retry_count = 0
child_orchestrator_count = 0
child_orchestrator_string = ''
child_act_retry_count = 0
child_instance_id = 'childInstanceID'
input_data = 'Hi Counter!'
event_name = 'event1'
event_data = 'eventData'
non_existent_id_error = 'no such instance exists'

retry_policy = RetryPolicy(
    first_retry_interval=timedelta(seconds=1),
    max_number_of_attempts=3,
    backoff_coefficient=2,
    max_retry_interval=timedelta(seconds=10),
    retry_timeout=timedelta(seconds=100),
)

wfr = WorkflowRuntime()

@wfr.workflow
def hello_world_wf(ctx: DaprWorkflowContext, wf_input):
    print(f'{wf_input}')
    yield ctx.call_activity(hello_act, input=1)
    yield ctx.call_activity(hello_act, input=10)
    yield ctx.call_activity(hello_retryable_act, retry_policy=retry_policy)
    yield ctx.call_child_workflow(child_retryable_wf, retry_policy=retry_policy)
    yield ctx.call_child_workflow(child_wf, instance_id=child_instance_id)
    yield ctx.call_activity(hello_act, input=100)
    yield ctx.call_activity(hello_act, input=1000)


@wfr.workflow
def child_wf(ctx: DaprWorkflowContext):
    yield ctx.wait_for_external_event('event1')


@wfr.activity
def hello_act(ctx: WorkflowActivityContext, wf_input):
    global counter
    counter += wf_input
    print(f'New counter value is: {counter}!', flush=True)

@wfr.activity
def hello_retryable_act(ctx: WorkflowActivityContext):
    global retry_count
    if (retry_count % 2) == 0:
        print(f'Retry count value is: {retry_count}!', flush=True)
        retry_count += 1
        raise ValueError('Retryable Error')
    print(f'Retry count value is: {retry_count}! This print statement verifies retry', flush=True)
    retry_count += 1


@wfr.workflow(name="child_retryable_wf")
def child_retryable_wf(ctx: DaprWorkflowContext):
    global child_orchestrator_string, child_orchestrator_count
    if not ctx.is_replaying:
        child_orchestrator_count += 1
        print(f'Appending {child_orchestrator_count} to child_orchestrator_string!', flush=True)
        child_orchestrator_string += str(child_orchestrator_count)
    yield ctx.call_activity(
        act_for_child_wf, input=child_orchestrator_count, retry_policy=retry_policy
    )
    if child_orchestrator_count < 3:
        raise ValueError('Retryable Error')


@wfr.activity(name="act_for_child_wf")
def act_for_child_wf(ctx: WorkflowActivityContext, inp):
    global child_orchestrator_string, child_act_retry_count
    inp_char = chr(96 + inp)
    print(f'Appending {inp_char} to child_orchestrator_string!', flush=True)
    child_orchestrator_string += inp_char
    if child_act_retry_count % 2 == 0:
        child_act_retry_count += 1
        raise ValueError('Retryable Error')
    child_act_retry_count += 1


def main():
    wfr.start()
    wfc = DaprWorkflowClient()

    print('==========Start Counter Increase as per Input:==========')
    instance_id = wfc.schedule_new_workflow(
        workflow=hello_world_wf,
        input=input_data,
    )
    print(f'start_resp {instance_id}')

    # Sleep for a while to let the workflow run
    sleep(12)
    assert counter == 11
    assert retry_count == 2
    assert child_orchestrator_string == '1aa2bb3cc'

    # Pause Test
    wfc.pause_workflow(instance_id=instance_id)
    sleep(5)
    wf_state = wfc.get_workflow_state(instance_id=instance_id)
    print(f'Get response from hello_world_wf after pause call: {wf_state.runtime_status}')

    # Resume Test
    wfc.resume_workflow(instance_id=instance_id)
    sleep(5)
    wf_state = wfc.get_workflow_state(instance_id=instance_id)
    print(f'Get response from hello_world_wf after resume call: {wf_state.runtime_status}')

    sleep(1)
    # Raise event
    wfc.raise_workflow_event(
        instance_id=child_instance_id,
        event_name=event_name,
        data=event_data,
    )

    sleep(5)
    # Purge Test
    wfc.purge_workflow(instance_id=instance_id)
    try:
        wfc.get_workflow_state(instance_id=instance_id)
    except DaprInternalError as err:
        if non_existent_id_error in err._message:
            print('Instance Successfully Purged')

    # Kick off another workflow for termination purposes
    # This will also test using the same instance ID on a new workflow after
    # the old instance was purged
    instance_id = wfc.schedule_new_workflow(
        instance_id=instance_id,
        workflow=hello_world_wf,
        input=input_data,
    )
    print(f'start_resp {instance_id}')

    sleep(5)
    # Terminate Test
    wfc.terminate_workflow(instance_id=instance_id)
    sleep(5)
    wf_state = wfc.get_workflow_state(instance_id=instance_id)
    print(
        f'Get response from hello_world_wf '
        f'after terminate call: {wf_state.runtime_status}'
    )
    child_wf_state = wfc.get_workflow_state(instance_id=child_instance_id)
    print(
        f'Get response from child_wf '
        f'after terminate call: {child_wf_state.runtime_status}'
    )

    # Purge Test
    wfc.purge_workflow(instance_id=instance_id)
    sleep(5)

    wf_state = wfc.get_workflow_state(instance_id=instance_id)
    if wf_state == None:
        print('Instance Successfully Purged')

    wfr.shutdown()


if __name__ == '__main__':
    main()
