import datetime
import json

from google.cloud import aiplatform_v1

def entry_point(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    SLA_SECONDS = 10
    print(event)
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    file = json.load(event.mediaLink)
    pipelines = []

    for entry in file:
        timestamp = entry['jsonPayload']['startTime']
        delta = datetime.now() - datetime.strptime(timestamp[:-11].replace('T',' '), '%Y-%m-%d %H:%M:%S')
        if delta.total_seconds() > SLA_SECONDS:
            pipelines.append(
                (
                entry['logName'].split('/')[1],
                entry['resource']['labels']['location'],
                entry['resource']['labels']['pipeline_job_id']
                )
            )

    if pipelines:
        client = aiplatform_v1.PipelineServiceClient()

    for pipeline in pipelines:
        name = f'projects/{pipeline[0]}/locations/{pipeline[1]}/pipelineJobs/{pipeline[2]}'
        request = aiplatform_v1.CancelPipelineJobRequest(name=name)
        client.cancel_pipeline_job(request=request)