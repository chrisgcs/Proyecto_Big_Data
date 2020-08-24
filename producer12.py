from time import sleep
from json import dumps
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['dist12.inf.santiago.usm.cl:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

f=open('yoochoose-clicks.dat', 'r')
for line in f:
    linea = line.split(",")
    data = {}
    data["sess"] = linea[0]
    data["date"] = linea[1].split("T")[0]
    data["time"] = linea[1].split("T")[1]
    data["prod"] = linea[2]
    data["cat"] = linea[3]
    producer.send('wordcounttopic', value=data)
    sleep(0.2)
f.close()

data = pd.read_csv("./dataset/yoochoose-clicks.dat", header=None, names=(['SessionID', 'TimeStamp', 'ItemID', 'Category']), low_memory=False)
lista = data.SessionID.unique()
df = []

for i in lista: #Aqui cambialo #for i in data['SessionID'] para que itere sobre todos los session_id 
    temp_df = data[data.SessionID == i]
    if(temp_df.empty): #para que cortara en la pruebas cuando encontrara un i que no existe
        break
    #Datos nuevos en el ultimo dia registrado en la sesion + 1 dia
    new_time = pd.to_datetime(max(temp_df['TimeStamp'].tolist())) + pd.to_timedelta(1, unit='d')
    new_item = random.choice(temp_df['ItemID'].tolist())
    new_category = random.choice(temp_df['Category'].tolist())
    data_gen = {}
    data_gen["sess"] = temp_df['SessionID'].tolist()[0]
    data_gen["date"] = str(new_time).split(" ")[0]
    data_gen["time"] = str(new_time).split(" ")[1]
    data_gen["prod"] = new_item
    data_gen["cat"] = new_category
    producer.send('wordcounttopic', value=data_gen)
    sleep(0.2)

    #new_row = [temp_df['SessionID'].tolist()[0], str(new_time), new_item, new_category]
    #df.append(new_row)
    
    for i in range(6): #Aqui van la cantidad de datos que quieres replicar, le aumenta 1 minuto
        new_time = new_time + pd.to_timedelta(1, unit='m')
        new_item = random.choice(temp_df['ItemID'].tolist())
        new_category = random.choice(temp_df['Category'].tolist())

        data_gen = {}
        data_gen["sess"] = temp_df['SessionID'].tolist()[0]
        data_gen["date"] = str(new_time).split(" ")[0]
        data_gen["time"] = str(new_time).split(" ")[1]
        data_gen["prod"] = new_item
        data_gen["cat"] = new_category
        producer.send('wordcounttopic', value=data_gen)
        sleep(0.2)
        #new_row = [temp_df['SessionID'].tolist()[0], str(new_time), new_item, new_category]
        #df.append(new_row)

sleep(10)
data2 = {}
data2["sess"] = ""
data2["date"] = ""
data2["time"] = ""
data2["prod"] = "end"
data2["cat"] = ""
producer.send('wordcounttopic', value=data2)
sleep(3)