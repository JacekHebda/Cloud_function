import datetime
import json
import logging

from google.cloud import aiplatform_v1


def entry_point(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    SLA_SECONDS = 10
    logging.info(event)
    logging.info('Event ID: {}'.format(context.event_id))
    logging.info('Event type: {}'.format(context.event_type))
    logging.info('Bucket: {}'.format(event['bucket']))
    logging.info('File: {}'.format(event['name']))
    logging.info('Metageneration: {}'.format(event['metageneration']))
    logging.info('Created: {}'.format(event['timeCreated']))
    logging.info('Updated: {}'.format(event['updated']))

    file = json.load(event['mediaLink'])
    client = aiplatform_v1.PipelineServiceClient(        
        client_options={"api_endpoint":"europe-west1-aiplatform.googleapis.com"},
    )

    for entry in file:
        timestamp = entry['jsonPayload']['startTime']
        project_id = entry['logName'].split('/')[1]
        location = entry['resource']['labels']['location']
        pipeline_id = entry['resource']['labels']['pipeline_job_id']
        name = f'projects/{project_id}/locations/{location}/pipelineJobs/{pipeline_id}'

        request = aiplatform_v1.GetPipelineJobRequest(name=name)
        pipeline = client.get_pipeline_job(request=request)

        logging.info(pipeline.labels)
        
        delta = datetime.now() - datetime.strptime(timestamp[:-11].replace('T',' '), '%Y-%m-%d %H:%M:%S')
        if delta.total_seconds() > pipeline.labels['sla']:
            request = aiplatform_v1.CancelPipelineJobRequest(name=name)
            client.cancel_pipeline_job(request=request)
