from botocore.config import Config
import boto3
import time
import datetime
import random
import numpy as np
import sys
import json
import argparse
#import Constant

LOOP_NUM = 1200
#def __init__(self):
def _submit_batch(location,records, counter):
    DATABASE_NAME="iot"
    TABLE_NAME="pm25"
    try:
        result = client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME,
                                               Records=records, CommonAttributes={})
        print ("Collection the data from the location ", location)
        print("Processed [%d] records. WriteRecords Status: [%s]" % (counter,
                                                                     result['ResponseMetadata']['HTTPStatusCode']))
    except Exception as err:
        print("Error:", err)


def _current_milli_time():
    return int(round(time.time() * 1000))

 
def city_insert(city,locations,pm25perloations,kinesis_client,stream_name):
    records = []
    counter = 0
    current_time = _current_milli_time()
    for location in locations:
        pm25forlocation = pm25perloations[location]
        measure_value = random.randint(pm25forlocation-5,pm25forlocation+5)
        record  = {
            'city': city,
            'location': location,
            'MeasureName': 'pm2.5',
            'MeasureValue': str(measure_value),
            'MeasureValueType': 'BIGINT',
            'Time': str(current_time)
            }
        data = json.dumps(record)
        records.append({'Data': bytes(data, 'utf-8'), 'PartitionKey': city})   
        counter = counter + 1
    
    kinesis_client.put_records(StreamName=stream_name, Records=records)
    print("collect city {} PM 2.5 metrics from {} locations to Kinesis Stream '{}'".format(city,len(locations), stream_name))

  

if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='timestream_kinesis_iot_pm2.5_data_gen',
                                     description='IoT timestream/KDA iot pm2.5 Sample Application.')
    parser.add_argument('--stream', action="store", type=str, default="Timestreampm25Stream",
                        help="The name of Kinesis Stream.")
    parser.add_argument('--region', '-e', action="store", choices=['us-east-1', 'us-east-2', 'us-west-2', 'eu-west-1'],
                        default="us-east-1", help="Specify the region of the Kinesis Stream.")

    args = parser.parse_args()
    stream_name = args.stream
    region_name = args.region
    kinesis_client = boto3.client('kinesis', region_name=region_name)

    try:
        kinesis_client.describe_stream(StreamName=stream_name)
    except:
        print("Unable to describe Kinesis Stream '{}' in region {}".format(stream_name, region_name))
        sys.exit(0)
    start_time = time.time()
    loop_start_time = time.strftime('%m-%d %H:%M:%S', time.localtime())
    print ("loop_start_time", loop_start_time)
    for i in range(LOOP_NUM):
        locations = ["yanshan","miyun","changping","mengtougou","huairou","aoti","nongzhan","haidian",
        "tongzhou","dingling","yanqing","guanyuan","dongsi","shunyi","fengtai_xiaotun","fengtai_yungang",
        "daxing","wanshou","gucheng","tiantan"]
        pm25perloations = {"yanshan":95,"miyun":75,"changping":70,"mengtougou":63,"huairou":60,"aoti":59,
        "nongzhan":55,"haidian":54,"tongzhou":53,"dingling":51,"yanqing":46,
        "guanyuan":44,"dongsi":38,"shunyi":33,"fengtai_xiaotun":170,"fengtai_yungang":132,"daxing":125,
        "wanshou":116,"gucheng":110,"tiantan":108}
        city_insert('Beijing',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["songjiang","fengxian","no 15 factory","xujing","pujiang",
        "putuo","xianxia","jingan","shangshida","hongkou","jiading","miaohang",
        "zhangjiang","yangpu","huinan","chongming"]
        pm25perloations = {"songjiang":70,"fengxian":73,"no 15 factory":63,"xujing":61,"pujiang":59,
        "putuo":57,"xianxia":53,"jingan":53,"shangshida":53,"hongkou":52,"jiading":52,"miaohang":47,
        "zhangjiang":47,"yangpu":39,"huinan":37,"chongming":14}
        city_insert('Shanghai',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["panyu","commercial school","No 5 middle school",
        "nansha","guangzhou monitor station","nansha street","No 86 middle school",
        "luhu","tiyu west","huangpu","jiulong town","chong hua","maofeng mountain",
        "baiyun","huadu"]
        pm25perloations = {"panyu":72,"commercial school":70,"No 5 middle school":68,
        "nansha":64,"guangzhou monitor station":64,"nansha street":64,"No 86 middle school":63,
        "luhu":63,"tiyu west":63,"huangpu":57,"jiulong town":57,"chong hua":50,"maofeng mountain":50,
        "baiyun":49,"huadu":46}
        city_insert('Guangzhou',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["guanlan","honghu","longgang","pingshan","henggang","minzhi","lianhua",
        "yantian","meisha","nanou","huaqiao city","xixiang"]
        pm25perloations = {"guanlan":99,"honghu":88,"longgang":88,"pingshan":66,"henggang":64,
        "minzhi":61,"lianhua":59,"yantian":56,"meisha":43,"nanou":43,"huaqiao city":114,"xixiang":105}
        city_insert('Shenzhen',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["shahepu","huayang","sanwayao","linjiang RD","Jinbo RD",
        "science city","junping ST","longquan"]
        pm25perloations = {"shahepu":82,"huayang":82,"sanwayao":72,"linjiang RD":70,"Jinbo RD":69,
        "science city":68,"junping ST":67,"longquan":58}   
        city_insert('Chengdu',locations,pm25perloations,kinesis_client, stream_name) 
        locations = ["linping town","xiasha","province center","hemu primary school","No 2 school",
        "Agriculture University","binjiang","xixi","yunxi","wolong bridge"]
        pm25perloations = {"linping town":54,"xiasha":34,"province center":31,"hemu primary school":24,"No 2 school":24,
        "Agriculture University":19,"binjiang":19,"xixi":14,"yunxi":15,"wolong bridge":15} 
        city_insert('Hangzhou',locations,pm25perloations,kinesis_client, stream_name) 
        locations = ["chaochang Gate","gaochun","lishui","xianlin",
        "jiangning","shanxi RD","pukou","liuhe"]
        pm25perloations = {"chaochang Gate":84,"gaochun":188,"lishui":155,"xianlin":132,
        "jiangning":129,"shanxi RD":126,"pukou":111,"liuhe":111}  
        city_insert('Nanjing',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["tuanbowa","binshui East RD","hexi RD","hanbei RD",
        "beihuan RD","jingu RD","diwei RD","xinlao RD"]
        pm25perloations = {"tuanbowa":99,"binshui East RD":76,"hexi RD":71,"hanbei RD":69,
        "beihuan RD":57,"jingu RD":51,"diwei RD":171,"xinlao RD":102}
        city_insert('Tianjin',locations,pm25perloations,kinesis_client, stream_name)  
        locations = ["hufeng","zhoujiaba","airport","xishan",
        "baiyi","changyuan","chongqing center","renmin east RD"]
        pm25perloations = {"hufeng":87,"zhoujiaba":61,"airport":63,"xishan":37,
        "baiyi":121,"changyuan":115,"chongqing center":104,"renmin east RD":62}   
        city_insert('Chongqing',locations,pm25perloations,kinesis_client, stream_name)  
        locations = ["Lasa monitor station","Lasa train station"]
        pm25perloations = {"Lasa monitor station":20,"Lasa train station":10} 
        city_insert('Lasa',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["Hexi station","Hedong station","Hyatt station"]
        pm25perloations = {"Hexi station":33,"Hedong station":31,"Hyatt station":33} 
        city_insert('Sanya',locations,pm25perloations,kinesis_client, stream_name)
        locations = ["Hunan","taiyuan St","donglin","wenhua ST","forest ST"]
        pm25perloations = {"Hunan":301,"taiyuan St":293,"donglin":289,"wenhua ST":276,"forest ST":169} 
        city_insert('Shenyang',locations,pm25perloations,kinesis_client, stream_name)
        time.sleep(random.randint(1,3))
    loop_end_time = time.strftime('%m-%d %H:%M:%S', time.localtime())
    print ("loop_end_time", loop_end_time)
