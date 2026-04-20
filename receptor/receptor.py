import boto3
import json
import time
import uuid

REGION = "us-east-1"
QUEUE_NAME = "cola-boletines"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:292177516549:notificacion-boletines"
TABLE_NAME = "boletines_reporte"

sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
tabla = dynamodb.Table(TABLE_NAME)

def procesar_mensaje(msg):
    datos = json.loads(msg)
    boletin_id = datos.get('id', str(uuid.uuid4()))
    correo = datos.get('correo')
    try:
        existente = tabla.get_item(Key={'boletin_id': boletin_id})
        if 'Item' in existente:
            print(f"Mensaje duplicado({boletin_id})")
            return
    except Exception as e:
        print(f"Error consultando la DB: {e}")
        
    tabla.put_item(
        Item={
            'boletin_id': boletin_id,
            'mensaje': datos.get('mensaje'),
            'correo': datos.get('correo'),
            'url_imagen': datos.get('url_imagen'),
            'leido': False
        }
    )
    URL_MOSTRADOR = f"http://54.209.149.157:8001/boletines/{boletin_id}?correoElectronico={correo}"
    mensaje_correo = (
        f"Se ha generado un nuevo boletin.\n\n"
        f"Contenido: {datos.get('mensaje')}\n"
        f"{URL_MOSTRADOR}\n\n"
        f"ID del Boletín: {boletin_id}"
    )
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=mensaje_correo,
        Subject="Nuevo Boletin Generado"
    )


def consumir():
    try:
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
        print(f"Monitoreando mensajes")
    except Exception as e:
        print(f"Error inicial: {e}")
        return

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
    
        messages = response.get('Messages', [])
        for msg in messages:
            try:
                procesar_mensaje(msg['Body'])
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=msg['ReceiptHandle']
                )
            except Exception as e:
                print(f"Error procesando mensaje: {e}")
        time.sleep(1)


if __name__ == "__main__":
    print("Servicio receptor listo")
    consumir()
    #prueba
